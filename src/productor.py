"""B.2 Productor: los 7.200 eventos en vivo → stream `sensores:t2` a 20×.

El generador es el mismo de la carga histórica: se salta los primeros 864.000
eventos y produce los 7.200 siguientes (la hora 2026-11-07 00:00–01:00). El
reloj *simulado* avanza 0,5 s por evento, pero el reloj real solo espera 25 ms:
así la hora entera se emite en ~3 minutos.

MAXLEN: 10.000 entradas ≈ 250 s (4 min) de stream a 40 ev/s. Es holgadamente
mayor que el lag normal de los consumidores (< 1 s). Si un consumidor se cae, el
stream guarda 4 minutos de eventos; más atrás, XADD recorta y esos eventos se
pierden para ese grupo (Redis avisa con el campo de recortes, pero no los
recupera). La historia ya cargada en HBase no se ve afectada.
"""
from __future__ import annotations

import argparse
import itertools
import time

from comun import (
    N_HIST,
    N_VIVO,
    PASO,
    RITMO_VIVO,
    SEMILLA,
    STREAM,
    T0,
    eventos,
    redis_conexion,
)


def main() -> None:
    ap = argparse.ArgumentParser(description="Productor en vivo → sensores:t2")
    ap.add_argument("--ritmo", type=float, default=RITMO_VIVO,
                    help="segundos reales entre eventos (default 0.025 = 20×)")
    ap.add_argument("--maxlen", type=int, default=10_000,
                    help="recorte aproximado del stream (default 10.000)")
    ap.add_argument("--limite", type=int, default=N_VIVO)
    args = ap.parse_args()

    r = redis_conexion()
    r.delete("t2:producidos", "t2:fin")
    r.hset("t2:meta", mapping={"ritmo_segundos": f"{args.ritmo}", "maxlen": str(args.maxlen)})

    print(f"== Productor == stream {STREAM} · {args.limite:,} eventos · "
          f"1 cada {args.ritmo * 1000:.0f} ms · MAXLEN ~{args.maxlen:,}", flush=True)

    gen = itertools.islice(
        eventos(T0, semilla=SEMILLA, paso=PASO), N_HIST, N_HIST + args.limite
    )
    # El generador es perezoso: adelantar los 864.000 históricos ANTES de medir,
    # para que el ritmo real de la hora en vivo no incluya ese costo.
    primero = next(gen)
    inicio = time.monotonic()
    for i, ev in enumerate(itertools.chain([primero], gen), start=1):
        r.xadd(STREAM, ev, maxlen=args.maxlen, approximate=True)
        r.incr("t2:producidos")
        if i % 720 == 0:
            transcurrido = time.monotonic() - inicio
            print(f"  {i:,}/{args.limite:,} · {transcurrido:.1f} s · "
                  f"{i / transcurrido:.1f} ev/s reales", flush=True)
        # Reloj real sin deriva acumulada.
        objetivo = inicio + i * args.ritmo
        time.sleep(max(0.0, objetivo - time.monotonic()))

    transcurrido = time.monotonic() - inicio
    r.set("t2:fin", str(time.time()))
    r.hset("t2:meta", mapping={
        "vivo_eventos": str(args.limite),
        "vivo_segundos": f"{transcurrido:.2f}",
        "vivo_eventos_por_segundo": f"{args.limite / transcurrido:.1f}",
    })
    print(f"Productor terminado: {args.limite:,} eventos en {transcurrido:.1f} s "
          f"({args.limite / transcurrido:.1f} ev/s reales, "
          f"×{(args.limite / transcurrido) * PASO:.0f} el ritmo simulado)", flush=True)


if __name__ == "__main__":
    main()
