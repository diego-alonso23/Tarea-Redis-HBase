"""P2 — Ventana de 10 minutos, desde Redis.

Para cada estación de flujo vehicular calcula el promedio de los últimos 10
minutos de **medición**, anclados a la medición más reciente del stream
(`tablero:max_ts`, que mantiene el consumidor del tablero).

Lee únicamente los sorted sets `ventana:<estacion>` (no usa los datos en
memoria ni HBase). El score es el timestamp de medición en ms; el miembro es
`ts:valor` para poder recuperar el valor sin otra estructura.

    python3 src/p2_ventana.py
"""
from __future__ import annotations

from comun import (
    ESTACIONES_FLUJO,
    N_HIST,
    VENTANA_MS,
    redis_conexion,
)
from util_medicion import medir, tabla_tiempos


def promedios(r):
    ancla = int(r.get("tablero:max_ts") or 0)
    desde = ancla - VENTANA_MS
    resultado = {}
    pipe = r.pipeline(transaction=False)
    for est in ESTACIONES_FLUJO:
        pipe.zrangebyscore(f"ventana:{est}", desde, ancla)
    for est, miembros in zip(ESTACIONES_FLUJO, pipe.execute()):
        valores = [float(m.split(":")[1]) for m in miembros]
        resultado[est] = {
            "n": len(valores),
            "promedio": sum(valores) / len(valores) if valores else float("nan"),
        }
    return ancla, resultado


def main() -> None:
    r = redis_conexion()
    ancla, _ = promedios(r)
    (_, promedios_final), mediana, tiempos = medir(lambda: promedios(r))

    print("== P2 · Ventana de 10 minutos (Redis) ==\n")
    print(f"Ancla = medición más reciente del stream: {ancla} ms")
    print(f"Ventana = [{ancla - VENTANA_MS}, {ancla}] ms (10 minutos)\n")
    print(f"{'estación':<10}{'lecturas':>10}{'promedio':>12}")
    for est in ESTACIONES_FLUJO:
        d = promedios_final[est]
        print(f"{est:<10}{d['n']:>10}{d['promedio']:>12.1f}")

    print()
    print(tabla_tiempos("Redis (8 ZRANGEBYSCORE)", mediana, tiempos))

    print("\nMemoria de la ventana (MEMORY USAGE)")
    total = 0
    for est in ESTACIONES_FLUJO:
        uso = r.memory_usage(f"ventana:{est}")
        n = r.zcard(f"ventana:{est}")
        total += uso
        print(f"  ventana:{est:<9}{n:>4} entradas{uso:>10,} bytes")

    # Extrapolación: la ventana recortada guarda 10 min; sin recorte crecería
    # con toda la historia (5 días) o, en producción, para siempre.
    por_entrada = total / sum(r.zcard(f"ventana:{e}") for e in ESTACIONES_FLUJO)
    # 5 días ≈ 864.000 eventos; la mitad son de flujo (8 de 16 estaciones).
    sin_recorte_5d = por_entrada * (N_HIST / 2) / 1e6
    print(f"\n  total actual: {total:,} bytes en {len(ESTACIONES_FLUJO)} claves")
    print(f"  ≈ {por_entrada:.0f} bytes por entrada")
    print(f"  sin recorte, 5 días de historia ≈ {sin_recorte_5d:,.1f} MB "
          f"(y en producción crecería sin techo)")

    print("\n¿Por qué un sorted set con el timestamp como score?")
    print("  - Insertar es O(log N) y el score ordena por tiempo.")
    print("  - ZRANGEBYSCORE responde la ventana en O(log N + M), sin recorrer")
    print("    la historia: se lee solo lo que cae entre ancla-600s y ancla.")
    print("  - ZREMRANGEBYSCORE recorta lo viejo en O(log N + M): la memoria")
    print("    queda acotada por el tiempo de la ventana, no por la historia.")


if __name__ == "__main__":
    main()
