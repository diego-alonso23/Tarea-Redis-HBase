"""B.2 Consumidor `tablero`: mantiene el "ahora" en Redis.

Estructuras (nombres exactos que pide el enunciado):
  * `ultimo:<estacion>`  hash {valor, ts (ms de medición), tipo}
  * `ranking:flujo`      zset estacion → último valor (solo flujo vehicular)
  * `ventana:<estacion>` zset miembro `ts:valor` → score ts (ms), últimos 10 min
  * `tipo:<tipo>`        set de estaciones de ese tipo (catálogo)

"Último" = último por **medición**, no por llegada. Antes de pisar `ultimo:*`
se compara el ts de medición contra el guardado: una lectura tardía (hasta 2 min
vieja) no hace retroceder el tablero ni el ranking. La ventana se recorta contra
la medición más reciente del stream (no contra la llegada), así que un evento
tardío que cae dentro de los 10 minutos sí entra, pero uno más viejo no revive.
"""
from __future__ import annotations

import argparse
import time

from comun import (
    CONSUMIDOR_TABLERO,
    ESTACIONES_FLUJO,
    GRUPO_TABLERO,
    STREAM,
    VENTANA_MS,
    redis_conexion,
    ts_ms,
)

LOTE = 200


def crear_grupo(r) -> None:
    try:
        r.xgroup_create(STREAM, GRUPO_TABLERO, id="0", mkstream=True)
        print(f"grupo '{GRUPO_TABLERO}' creado", flush=True)
    except Exception as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def main() -> None:
    ap = argparse.ArgumentParser(description="Consumidor del tablero (Redis)")
    ap.add_argument("--limite", type=int, default=0,
                    help="cortar tras N entradas (0 = hasta que el productor termine)")
    args = ap.parse_args()

    r = redis_conexion()
    crear_grupo(r)
    print(f"== Consumidor tablero == grupo {GRUPO_TABLERO} · "
          f"consumidor {CONSUMIDOR_TABLERO}", flush=True)

    max_ts = 0
    procesadas = 0
    siguiente_aviso = 1_000
    inicio = time.perf_counter()

    while True:
        respuesta = r.xreadgroup(
            GRUPO_TABLERO, CONSUMIDOR_TABLERO, {STREAM: ">"}, count=LOTE, block=2_000
        )
        if not respuesta:
            if r.exists("t2:fin") or (args.limite and procesadas >= args.limite):
                break
            continue

        entradas = respuesta[0][1]

        # 1) ¿avanzó la medición más reciente del stream? recortar todas las ventanas
        batch_max = max(ts_ms(ev) for _id, ev in entradas)
        if batch_max > max_ts:
            max_ts = batch_max
            corte = max_ts - VENTANA_MS
            pipe = r.pipeline(transaction=False)
            for est in ESTACIONES_FLUJO:
                pipe.zremrangebyscore(f"ventana:{est}", 0, corte)
            pipe.set("tablero:max_ts", max_ts)
            pipe.execute()

        pipe = r.pipeline(transaction=False)
        vistas: dict[str, int] = {}          # último ts aceptado por estación en este lote
        for _id, ev in entradas:
            est = ev["estacion_id"]
            tipo = ev["tipo_sensor"]
            valor = ev["valor"]
            ts = ts_ms(ev)

            pipe.sadd(f"tipo:{tipo}", est)

            previo = vistas.get(est)
            if previo is None:
                guardado = r.hget(f"ultimo:{est}", "ts")
                previo = int(guardado) if guardado is not None else None
            if previo is not None and ts < previo:
                continue                     # lectura tardía: el tablero no retrocede
            vistas[est] = ts

            pipe.hset(f"ultimo:{est}", mapping={
                "valor": valor, "ts": ts, "tipo": tipo,
            })
            if tipo == "flujo_vehicular":
                pipe.zadd("ranking:flujo", {est: float(valor)})
                if ts >= max_ts - VENTANA_MS:
                    # una sola lectura por (estación, ts): si hay choque, gana la última
                    pipe.zremrangebyscore(f"ventana:{est}", ts, ts)
                    pipe.zadd(f"ventana:{est}", {f"{ts}:{valor}": ts})

        pipe.execute()
        r.xack(STREAM, GRUPO_TABLERO, *[i for i, _ in entradas])
        r.incrby("t2:procesados:tablero", len(entradas))
        procesadas += len(entradas)

        if procesadas >= siguiente_aviso:
            print(f"  {procesadas:,} entradas · "
                  f"{time.perf_counter() - inicio:.1f} s", flush=True)
            siguiente_aviso += 1_000

    transcurrido = time.perf_counter() - inicio
    pendientes = r.xpending(STREAM, GRUPO_TABLERO)["pending"]
    print(f"Tablero terminado: {procesadas:,} entradas en {transcurrido:.1f} s "
          f"· XPENDING={pendientes}", flush=True)


if __name__ == "__main__":
    main()
