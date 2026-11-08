"""P6 — Pregunta propia: ¿qué estaciones están hoy fuera de su historia?

Pregunta: **de las 5 estaciones de flujo con más flujo ahora, ¿cuánto se
desvía el valor actual de la media y la desviación histórica de sus 5 días?**

Reparto:
  * Redis decide el "qué" y el "ahora": `ranking:flujo` elige las 5 estaciones
    y `ultimo:<est>` da el valor actual (último por medición).
  * HBase decide el "cuándo" y la escala: la media/desviación de 5 días no cabe
    en Redis sin traerla desde disco; se calcula con un scan por estación.

No es una variante de P1 (que solo compara el último valor entre motores): acá
el valor actual se contrasta contra la distribución histórica de cada estación.

    python3 src/p6_propia.py
"""
from __future__ import annotations

import statistics
import time

from comun import FAMILIA, TABLA_LECTURAS, hbase_conexion, redis_conexion
from util_medicion import medir, tabla_tiempos

COL_VALOR = f"{FAMILIA}:valor".encode()


def top5_redis(r):
    return [(est, float(v)) for est, v in r.zrevrange("ranking:flujo", 0, 4, withscores=True)]


def historia_hbase(tabla, est: str):
    valores = []
    for _clave, cols in tabla.scan(
        row_start=f"{est}#".encode(), row_stop=f"{est}#~".encode(),
        columns=[COL_VALOR],
    ):
        valores.append(float(cols[COL_VALOR]))
    n = len(valores)
    media = sum(valores) / n
    desv = statistics.pstdev(valores)
    return n, media, desv


def main() -> None:
    r = redis_conexion()
    conn = hbase_conexion()
    tabla = conn.table(TABLA_LECTURAS)

    top = top5_redis(r)
    print("== P6 · Pregunta propia: ahora (Redis) vs historia (HBase) ==\n")
    print("Las 5 con más flujo ahora (redis ranking:flujo):")
    for est, v in top:
        print(f"  {est:<10}{v:>8.1f}")

    print("\nEstadística histórica de 5 días (HBase, scan por estación):")
    print(f"{'estación':<10}{'n':>8}{'media':>9}{'desv':>8}{'ahora':>8}{'z':>8}  lectura")
    inicio = time.perf_counter()
    for est, actual in top:
        n, media, desv = historia_hbase(tabla, est)
        z = (actual - media) / desv if desv else 0.0
        etiqueta = "normal" if abs(z) <= 2 else "ANÓMALA"
        print(f"{est:<10}{n:>8,}{media:>9.2f}{desv:>8.2f}{actual:>8.1f}{z:>8.2f}  {etiqueta}")
    total_seg = time.perf_counter() - inicio

    # tiempo medible: otra corrida con mediana de 5 (5 estaciones × 5 corridas)
    def _consulta():
        return [historia_hbase(tabla, est) for est, _ in top]

    _, mediana, tiempos = medir(_consulta, corridas=3, calentar=False)
    print()
    print(tabla_tiempos("Redis top5 + HBase historia", mediana, tiempos))
    print(f"\nUna pasada de {sum(1 for _ in top)} estaciones: {total_seg:.2f} s")

    print("\nPor qué este reparto necesita los dos motores:")
    print("  - Redis responde 'cuáles y cuánto ahora' sin tocar disco (ranking y")
    print("    ultimo:* ya están mantenidos por el consumidor del tablero).")
    print("  - HBase responde 'cuál es su historia' porque los 5 días no viven en")
    print("    memoria y requieren recorrer las filas de cada estación.")
    print("  - Hacerlo solo en Redis obligaría a mantener 5 días de estadísticas")
    print("    por estación en memoria (lo que la ventana de 10 min evita).")
    print("  - Hacerlo solo en HBase obligaría a definir 'ahora' con un scan")
    print("    invertido por estación y sin el ranking ya calculado.")

    conn.close()


if __name__ == "__main__":
    main()
