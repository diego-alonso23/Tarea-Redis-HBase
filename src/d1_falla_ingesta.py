"""D.1 — El consumidor muere a mitad de lote (después del put, antes del XACK).

Reproduce el experimento sobre el stream real:
  1. reinicia el grupo `ingesta` (la historia en HBase no se toca);
  2. corre `consumidor_ingesta.py --matar-en-lote 1`: escribe el primer lote de
     200 entradas en HBase y llama os._exit(1) antes del XACK;
  3. muestra XPENDING (200 entradas a nombre de ingesta-1);
  4. un segundo consumidor las recupera con XAUTOCLAIM y sigue con el resto;
  5. verifica que HBase tiene exactamente 853.953 filas (ni una más ni una menos)
     y muestra las dos versiones que quedaron en una celda reprocesada.

No requiere volver a cargar el histórico: las filas son idempotentes.

    python3 src/d1_falla_ingesta.py
"""
from __future__ import annotations

import subprocess
import sys

from comun import (
    CONTROL,
    FAMILIA,
    GRUPO_INGESTA,
    STREAM,
    TABLA_LECTURAS,
    TABLA_TIPO,
    hbase_conexion,
    hbase_shell,
    redis_conexion,
    row_lecturas,
)


def correr(args: list[str]) -> int:
    return subprocess.run([sys.executable, *args]).returncode


def contar(tabla) -> int:
    return sum(1 for _ in tabla.scan(
        filter=b"FirstKeyOnlyFilter() AND KeyOnlyFilter()", batch_size=1_000
    ))


def main() -> None:
    r = redis_conexion()
    print("== D.1 · Consumidor muere entre put y XACK ==\n")

    if r.xlen(STREAM) < CONTROL["vivo"]["eventos"]:
        raise SystemExit("El stream no tiene la hora en vivo completa: corre ./run.sh primero")

    # Punto de partida controlado: mismo stream, grupo nuevo.
    try:
        r.xgroup_destroy(STREAM, GRUPO_INGESTA)
    except Exception:
        pass
    r.delete("t2:procesados:ingesta")
    print("Grupo 'ingesta' reiniciado (la historia en HBase no se modifica).")

    # 1) El consumidor escribe el primer lote y muere antes de XACK.
    rc = correr(["src/consumidor_ingesta.py", "--matar-en-lote", "1"])
    print(f"Consumidor 1 terminó con código {rc} (1 = muerte simulada)\n")

    resumen = r.xpending(STREAM, GRUPO_INGESTA)
    detalle = r.xpending_range(STREAM, GRUPO_INGESTA, "-", "+", 10)
    print(f"XPENDING: {resumen['pending']} entradas "
          f"(mínimo {resumen['min']} · máximo {resumen['max']})")
    print(f"  consumidor(es) con pendientes: "
          f"{sorted({d['consumer'] for d in detalle})}")
    print(f"  primeras entradas pendientes: {[d['message_id'] for d in detalle[:3]]}\n")

    # 2) Recuperación con otro consumidor (XAUTOCLAIM) y resto del stream.
    print("Consumidor 2 recupera los pendientes y continúa…")
    rc2 = correr(["src/consumidor_ingesta.py", "--recuperar",
                  "--consumidor", "ingesta-2"])
    print(f"Consumidor 2 terminó con código {rc2}")
    print(f"XPENDING final: {r.xpending(STREAM, GRUPO_INGESTA)['pending']}\n")

    # 3) HBase no ganó ni perdió filas.
    conn = hbase_conexion()
    print("Filas en HBase tras reprocesar:")
    for nombre in (TABLA_LECTURAS, TABLA_TIPO):
        n = contar(conn.table(nombre))
        esperado = CONTROL["total"]["claves"]
        print(f"  {nombre:<16}{n:>10,}  esperado {esperado:>10,}  "
              f"{'OK' if n == esperado else 'MAL'}")

    # 4) Versiones en una celda reprocesada (la primera entrada del lote).
    primera = r.xrange(STREAM, detail_min_id(detalle), detail_min_id(detalle))
    if primera:
        _id, ev = primera[0]
        clave = row_lecturas(ev).decode()
        print(f"\nVersiones en la celda {clave} (d:valor):")
        salida = hbase_shell(
            f"get '{TABLA_LECTURAS}', '{clave}', "
            f"{{COLUMN => '{FAMILIA}:valor', VERSIONS => 3}}\n"
        )
        for linea in salida.splitlines():
            if "value=" in linea or "COLUMN" in linea or "row(s)" in linea:
                print("  " + linea.strip())

    conn.close()
    print("\nPor qué no se duplica: la row key sale de la estación y del")
    print("milisegundo de medición, no del instante de llegada. Reprocesar el")
    print("lote vuelve a escribir la misma fila; HBase guarda una versión nueva")
    print("en la misma celda (VERSIONS=3), no una fila nueva. El XACK va después")
    print("del put justamente para que una muerte como esta deje el lote")
    print("pendiente y otro consumidor pueda recuperarlo.")


def detail_min_id(detalle):
    return detalle[0]["message_id"] if detalle else "-"


if __name__ == "__main__":
    main()
