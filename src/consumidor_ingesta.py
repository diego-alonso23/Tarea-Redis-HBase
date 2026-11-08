"""B.2 Consumidor `ingesta`: stream → HBase (las dos tablas).

Reglas del enunciado que se cumplen acá:
  * lee en lotes con XREADGROUP (grupo propio, independiente de `tablero`);
  * escribe en HBase en batch;
  * recién después del put hace XACK (nunca antes);
  * la row key sale de la estación y de la medición, así que reprocesar un lote
    no duplica filas: sobrescribe la misma celda (idempotencia).

Uso normal:
    python3 src/consumidor_ingesta.py

Recuperar pendientes de un consumidor muerto (Parte D.1):
    python3 src/consumidor_ingesta.py --recuperar

Muerte controlada para el experimento D.1 (después del put, antes del XACK):
    python3 src/consumidor_ingesta.py --matar-en-lote 1
"""
from __future__ import annotations

import argparse
import os
import time

from comun import (
    CONSUMIDOR_INGESTA,
    FAMILIA,
    GRUPO_INGESTA,
    STREAM,
    TABLA_LECTURAS,
    TABLA_TIPO,
    hbase_conexion,
    redis_conexion,
    row_lecturas,
    row_lecturas_tipo,
)

LOTE = 200


def crear_grupo(r) -> None:
    try:
        r.xgroup_create(STREAM, GRUPO_INGESTA, id="0", mkstream=True)
        print(f"grupo '{GRUPO_INGESTA}' creado", flush=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise

def escribir(tablas, entradas) -> int:
    """Un batch por tabla. Devuelve entradas escritas."""
    with tablas[TABLA_LECTURAS].batch(batch_size=len(entradas)) as b:
        for _id, ev in entradas:
            b.put(
                row_lecturas(ev),
                {
                    f"{FAMILIA}:tipo": ev["tipo_sensor"].encode(),
                    f"{FAMILIA}:valor": str(ev["valor"]).encode(),
                },
            )
    with tablas[TABLA_TIPO].batch(batch_size=len(entradas)) as b:
        for _id, ev in entradas:
            b.put(
                row_lecturas_tipo(ev),
                {f"{FAMILIA}:valor": str(ev["valor"]).encode()},
            )
    return len(entradas)


def recuperar_pendientes(r, tablas, consumidor: str) -> int:
    """XAUTOCLAIM: se queda con los pendientes de cualquier consumidor muerto."""
    recuperados = 0
    cursor = "0-0"
    while True:
        cursor, entradas, _borrados = r.xautoclaim(
            STREAM, GRUPO_INGESTA, consumidor, min_idle_time=0,
            start_id=cursor, count=LOTE,
        )
        if not entradas:
            break
        recuperados += escribir(tablas, entradas)
        r.xack(STREAM, GRUPO_INGESTA, *[i for i, _ in entradas])
        r.incrby("t2:procesados:ingesta", len(entradas))
        print(f"  recuperadas y re-escritas {len(entradas)} entradas "
              f"(idempotente)", flush=True)
        if cursor == "0-0":
            break
    return recuperados


def main() -> None:
    ap = argparse.ArgumentParser(description="Consumidor de ingesta (stream → HBase)")
    ap.add_argument("--consumidor", default=CONSUMIDOR_INGESTA)
    ap.add_argument("--recuperar", action="store_true",
                    help="antes de leer lo nuevo, reclamar pendientes (XAUTOCLAIM)")
    ap.add_argument("--matar-en-lote", type=int, default=0,
                    help="os._exit(1) tras el put del lote N, antes del XACK")
    args = ap.parse_args()

    r = redis_conexion()
    crear_grupo(r)
    conn = hbase_conexion()
    tablas = {n: conn.table(n) for n in (TABLA_LECTURAS, TABLA_TIPO)}

    if args.recuperar:
        n = recuperar_pendientes(r, tablas, args.consumidor)
        print(f"Recuperadas {n} entradas pendientes", flush=True)

    print(f"== Consumidor ingesta == grupo {GRUPO_INGESTA} · "
          f"consumidor {args.consumidor} · lote {LOTE}", flush=True)

    procesadas = 0
    lote_n = 0
    siguiente_aviso = 1_000
    inicio = time.perf_counter()

    while True:
        respuesta = r.xreadgroup(
            GRUPO_INGESTA, args.consumidor, {STREAM: ">"}, count=LOTE, block=2_000
        )
        if not respuesta:
            # Sin entradas nuevas: si el productor terminó, no queda nada por leer.
            if r.exists("t2:fin"):
                break
            continue

        entradas = respuesta[0][1]
        lote_n += 1
        escribir(tablas, entradas)              # 1) put
        if args.matar_en_lote and lote_n == args.matar_en_lote:
            print(f"MUERTE SIMULADA: lote {lote_n} escrito en HBase y sin XACK "
                  f"({len(entradas)} entradas quedan pendientes)", flush=True)
            os._exit(1)
        r.xack(STREAM, GRUPO_INGESTA, *[i for i, _ in entradas])  # 2) XACK
        r.incrby("t2:procesados:ingesta", len(entradas))
        procesadas += len(entradas)

        if procesadas >= siguiente_aviso:
            print(f"  {procesadas:,} entradas · "
                  f"{time.perf_counter() - inicio:.1f} s", flush=True)
            siguiente_aviso += 1_000

    transcurrido = time.perf_counter() - inicio
    pendientes = r.xpending(STREAM, GRUPO_INGESTA)["pending"]
    conn.close()
    print(f"Ingesta terminada: {procesadas:,} entradas en {transcurrido:.1f} s "
          f"({procesadas / transcurrido:,.0f} entradas/s) · XPENDING={pendientes}",
          flush=True)


if __name__ == "__main__":
    main()
