"""B.1 Carga histórica: 864.000 eventos directo a HBase, con batch.

No pasa por Redis: es la historia que ya existe. Escribe en las dos tablas
pre-divididas (`lecturas` y `lecturas_tipo`) y reporta tiempo y filas/s.

    python3 src/carga_historica.py              # desde cero (borra y recrea tablas)
    python3 src/carga_historica.py --keep       # conserva lo ya cargado
    python3 src/carga_historica.py --limite 100000   # prueba rápida

Las filas se deduplican dentro de cada lote (misma row key → se queda el último
valor). Los repetidos entre lotes los absorbe HBase: misma row key, sobrescribe.
"""
from __future__ import annotations

import argparse
import itertools
import time

from comun import (
    FAMILIA,
    N_HIST,
    PASO,
    SEMILLA,
    T0,
    TABLA_LECTURAS,
    TABLA_TIPO,
    crear_tabla,
    eventos,
    hbase_conexion,
    redis_conexion,
    row_lecturas,
    row_lecturas_tipo,
    splits_lecturas,
    splits_lecturas_tipo,
)


def preparar_tablas(reset: bool) -> None:
    crear_tabla(TABLA_LECTURAS, splits_lecturas(), reset)
    crear_tabla(TABLA_TIPO, splits_lecturas_tipo(), reset)


def volcar(tablas, lote: dict[bytes, dict]) -> int:
    """Escribe un lote en las dos tablas. Devuelve filas distintas escritas."""
    with tablas[TABLA_LECTURAS].batch(batch_size=len(lote)) as b:
        for ev in lote.values():
            b.put(
                row_lecturas(ev),
                {
                    f"{FAMILIA}:tipo": ev["tipo_sensor"].encode(),
                    f"{FAMILIA}:valor": str(ev["valor"]).encode(),
                },
            )
    with tablas[TABLA_TIPO].batch(batch_size=len(lote)) as b:
        for ev in lote.values():
            b.put(
                row_lecturas_tipo(ev),
                {f"{FAMILIA}:valor": str(ev["valor"]).encode()},
            )
    return len(lote)


def main() -> None:
    ap = argparse.ArgumentParser(description="Carga histórica a HBase (batch)")
    ap.add_argument("--limite", type=int, default=N_HIST, help="eventos a cargar")
    ap.add_argument("--batch", type=int, default=2_000, help="tamaño de lote")
    ap.add_argument("--keep", action="store_true", help="no borrar las tablas existentes")
    args = ap.parse_args()

    print("== Carga histórica ==", flush=True)
    print(f"Eventos a cargar: {args.limite:,} · batch {args.batch:,}", flush=True)
    creado = time.perf_counter()
    preparar_tablas(reset=not args.keep)
    print(f"Tablas listas en {time.perf_counter() - creado:.1f} s "
          f"(lecturas: {len(splits_lecturas()) + 1} regiones · "
          f"lecturas_tipo: {len(splits_lecturas_tipo()) + 1} regiones)", flush=True)

    conn = hbase_conexion()
    tablas = {n: conn.table(n) for n in (TABLA_LECTURAS, TABLA_TIPO)}

    gen = eventos(T0, semilla=SEMILLA, paso=PASO)
    inicio = time.perf_counter()
    procesados = escritos = 0
    lote: dict[bytes, dict] = {}
    ultimo_aviso = 0

    for ev in itertools.islice(gen, args.limite):
        procesados += 1
        lote[row_lecturas(ev)] = ev          # dedupe dentro del lote
        if len(lote) >= args.batch:
            escritos += volcar(tablas, lote)
            lote.clear()
        if procesados - ultimo_aviso >= 50_000:
            ultimo_aviso = procesados
            print(f"  {procesados:,} eventos · {escritos:,} filas · "
                  f"{time.perf_counter() - inicio:.1f} s", flush=True)

    if lote:
        escritos += volcar(tablas, lote)

    transcurrido = time.perf_counter() - inicio
    conn.close()

    print()
    print(f"Eventos procesados : {procesados:,}")
    print(f"Filas en lecturas  : {escritos:,}")
    print(f"Tiempo             : {transcurrido:.1f} s")
    print(f"Eventos/s          : {procesados / transcurrido:,.0f}")
    print(f"Filas/s (2 tablas) : {escritos / transcurrido:,.0f} "
          f"({2 * escritos / transcurrido:,.0f} mutaciones/s)")

    # Metadatos para verificar.py (no son datos del pipeline).
    try:
        r = redis_conexion()
        r.hset("t2:meta", mapping={
            "historico_eventos": str(procesados),
            "historico_filas": str(escritos),
            "historico_segundos": f"{transcurrido:.2f}",
            "historico_escrituras_por_segundo": f"{procesados / transcurrido:.0f}",
        })
    except Exception as exc:  # Redis puede no estar disponible en una carga aislada
        print(f"(aviso: no se guardaron metadatos en Redis: {exc})")


if __name__ == "__main__":
    main()
