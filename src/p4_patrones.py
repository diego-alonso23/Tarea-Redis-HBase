"""P4 — Dos patrones de acceso: catálogo en Redis + HBase.

Para cada estación de calidad del aire: promedio y máximo de PM2.5 entre las
07:00 y 09:00 del 05-nov, y la hora del máximo (si se repite, la primera).

  a) `lecturas` (estación primero): un rango por estación; la lista de
     estaciones sale del catálogo de Redis (`SMEMBERS tipo:calidad_aire`).
  b) `lecturas_tipo` (tipo primero): un solo scan para todas las estaciones.

    python3 src/p4_patrones.py
"""
from __future__ import annotations

import subprocess
from datetime import datetime

from comun import (
    FAMILIA,
    RAIZ,
    TABLA_LECTURAS,
    TABLA_TIPO,
    hbase_conexion,
    hbase_shell,
    redis_conexion,
)
from util_medicion import medir, tabla_tiempos

INI = "2026-11-05T07:00:00.000-03:00"
FIN = "2026-11-05T09:00:00.000-03:00"
COL_VALOR = f"{FAMILIA}:valor".encode()


def resumir(por_estacion: dict[str, list[tuple[str, float]]]):
    """{est: [(iso_ts, valor)]} → {est: (n, promedio, máximo, hora_del_máximo)}."""
    salida = {}
    for est, lecturas in por_estacion.items():
        lecturas.sort()
        valores = [v for _t, v in lecturas]
        maximo = max(valores)
        hora_max = next(t for t, v in lecturas if v == maximo)
        salida[est] = (
            len(valores),
            sum(valores) / len(valores),
            maximo,
            datetime.fromisoformat(hora_max).strftime("%H:%M:%S.%f")[:-3],
        )
    return salida


def por_estacion(tabla, estaciones):
    datos: dict[str, list[tuple[str, float]]] = {e: [] for e in estaciones}
    for est in estaciones:
        for clave, cols in tabla.scan(
            row_start=f"{est}#{INI}".encode(),
            row_stop=f"{est}#{FIN}".encode(),
            columns=[COL_VALOR],
        ):
            ts = clave.decode().split("#", 1)[1]
            datos[est].append((ts, float(cols[COL_VALOR])))
    return resumir(datos)


def por_tipo(tabla):
    datos: dict[str, list[tuple[str, float]]] = {}
    for clave, cols in tabla.scan(
        row_start=f"calidad_aire#{INI}".encode(),
        row_stop=f"calidad_aire#{FIN}".encode(),
        columns=[COL_VALOR],
    ):
        _tipo, ts, est = clave.decode().split("#", 2)
        datos.setdefault(est, []).append((ts, float(cols[COL_VALOR])))
    return resumir(datos)


def tamano_tabla(nombre: str) -> str:
    try:
        # Flush para que el tamaño en disco incluya lo que aún está en MemStore.
        hbase_shell(f"flush '{nombre}'\n")
        salida = subprocess.run(
            ["docker", "compose", "exec", "-T", "hbase", "du", "-sh",
             f"/data/hbase/data/default/{nombre}"],
            capture_output=True, text=True, cwd=str(RAIZ),
        )
        return salida.stdout.strip() or "n/d"
    except Exception:
        return "n/d"


def main() -> None:
    r = redis_conexion()
    conn = hbase_conexion()
    estaciones = sorted(r.smembers("tipo:calidad_aire"))
    print("== P4 · Dos patrones de acceso ==\n")
    print(f"Catálogo Redis tipo:calidad_aire → {estaciones}\n")

    t_lect = medir(lambda: por_estacion(conn.table(TABLA_LECTURAS), estaciones), corridas=1, calentar=False)
    (res_a, med_a, tiempos_a) = medir(
        lambda: por_estacion(conn.table(TABLA_LECTURAS), estaciones)
    )
    (res_b, med_b, tiempos_b) = medir(lambda: por_tipo(conn.table(TABLA_TIPO)))

    print(f"{'estación':<10}{'n':>6}{'promedio':>10}{'máximo':>9}{'hora del máximo':>18}")
    for est in estaciones:
        n, prom, maximo, hora = res_a[est]
        print(f"{est:<10}{n:>6}{prom:>10.2f}{maximo:>9.1f}{hora:>18}")

    print()
    print(tabla_tiempos("a) lecturas (3 rangos)", med_a, tiempos_a))
    print(tabla_tiempos("b) lecturas_tipo (1 scan)", med_b, tiempos_b))
    print(f"\nEl patrón tipo-primero es {med_a / med_b:.1f}× más rápido con "
          f"{len(estaciones)} estaciones; la brecha crece con el catálogo.")

    iguales = all(
        res_a[e][1:] == res_b[e][1:] for e in estaciones
    )
    print(f"Ambos patrones dan el mismo resultado: {iguales}")

    print("\nCosto de mantener la segunda tabla")
    print(f"  escrituras extra: 1 put más por evento (871.200 en total)")
    print(f"  espacio: lecturas {tamano_tabla(TABLA_LECTURAS)} · "
          f"lecturas_tipo {tamano_tabla(TABLA_TIPO)}")
    print("  consistencia: el consumidor hace XACK recién después de escribir")
    print("  las dos tablas. Si falla la segunda, el lote queda pendiente y se")
    print("  reprocesa; como la row key es idempotente, reescribir no duplica.")
    print("  Aun así, entre el put 1 y el put 2 hay una ventana en que `lecturas`")
    print("  ya tiene la fila y `lecturas_tipo` no. Se acepta porque el sistema es")
    print("  at-least-once y la consulta tolera ese retraso; si se exigiera")
    print("  atomicidad entre tablas habría que usar una transacción o un log.")
    print("  No valdría la pena si el catálogo de tipos fuera enorme y las")
    print("  consultas por tipo raras: se pagaría el doble de escritura siempre")
    print("  para ahorrar unos pocos scans.")

    conn.close()


if __name__ == "__main__":
    main()
