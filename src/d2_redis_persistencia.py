"""D.2 — Redis se reinicia: AOF con volumen vs sin persistencia.

Para no destruir el estado del pipeline principal, el experimento levanta dos
contenedores Redis efímeros con la MISMA carga (3.000 eventos, un grupo con 300
pendientes, ultimo:*, ranking y una ventana) y reinicia cada uno:

  * t2-d2-aof        redis-server --appendonly yes --appendfsync everysec
                     (idéntico a la decisión de la Parte A) + volumen con nombre
  * t2-d2-nopersist  redis-server --appendonly no --save "" (sin persistencia)

Después del `docker restart` se compara qué sobrevivió de cada estructura.

    python3 src/d2_redis_persistencia.py
"""
from __future__ import annotations

import itertools
import subprocess
import time

import redis

from comun import (
    N_HIST,
    PASO,
    SEMILLA,
    STREAM,
    T0,
    eventos,
    ts_ms,
)

AOF = ("t2-d2-aof", 6381)
NOPERSIST = ("t2-d2-nopersist", 6382)
EVENTOS = 3_000
PENDIENTES = 300


def docker(*args: str, check: bool = True):
    return subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=check
    )


def esperar(host: str, puerto: int, segundos: float = 30) -> redis.Redis:
    fin = time.time() + segundos
    while time.time() < fin:
        try:
            r = redis.Redis(host=host, port=puerto, decode_responses=True)
            r.ping()
            return r
        except Exception:
            time.sleep(0.5)
    raise RuntimeError(f"Redis en {host}:{puerto} no respondió")


def arrancar() -> None:
    for nombre, _puerto in (AOF, NOPERSIST):
        docker("rm", "-f", nombre, check=False)
    docker("volume", "rm", "t2_d2_aof", check=False)

    print("Levantando Redis AOF con volumen (6381)…")
    docker("run", "-d", "--name", AOF[0], "-p", f"{AOF[1]}:6379",
           "-v", "t2_d2_aof:/data", "redis:7",
           "redis-server", "--appendonly", "yes", "--appendfsync", "everysec",
           "--save", "")
    print("Levantando Redis sin persistencia (6382)…")
    docker("run", "-d", "--name", NOPERSIST[0], "-p", f"{NOPERSIST[1]}:6379",
           "redis:7", "redis-server", "--appendonly", "no", "--save", "")


def poblar(r: redis.Redis, evs) -> None:
    pipe = r.pipeline(transaction=False)
    for ev in evs:
        pipe.xadd(STREAM, ev, maxlen=10_000, approximate=True)
    pipe.execute()

    r.xgroup_create(STREAM, "ingesta", id="0")
    r.xreadgroup("ingesta", "c1", {STREAM: ">"}, count=PENDIENTES)  # sin XACK

    ultimo = evs[-1]
    r.hset("ultimo:CC-001", mapping={
        "valor": ultimo["valor"], "ts": ts_ms(ultimo), "tipo": "flujo_vehicular",
    })
    r.zadd("ranking:flujo", {"CC-001": float(ultimo["valor"]), "HUA-002": 97.1})
    for ev in evs[-100:]:
        if ev["estacion_id"] == "CC-001":
            r.zadd("ventana:CC-001", {f"{ts_ms(ev)}:{ev['valor']}": ts_ms(ev)})
    r.sadd("tipo:flujo_vehicular", "CC-001")


def foto(r: redis.Redis) -> dict:
    try:
        grupos = {g["name"]: int(g["pending"]) for g in r.xinfo_groups(STREAM)}
        stream = r.xlen(STREAM)
    except Exception:
        grupos, stream = {}, 0
    return {
        "stream": stream,
        "grupos": grupos,
        "ultimo": bool(r.exists("ultimo:CC-001")),
        "ventana": r.zcard("ventana:CC-001"),
        "ranking": r.zcard("ranking:flujo"),
    }


def main() -> None:
    print("== D.2 · Reinicio de Redis: persistencia ==\n")
    arrancar()
    try:
        ra = esperar("localhost", AOF[1])
        rn = esperar("localhost", NOPERSIST[1])

        evs = list(itertools.islice(
            eventos(T0, semilla=SEMILLA, paso=PASO), N_HIST, N_HIST + EVENTOS
        ))
        for r in (ra, rn):
            poblar(r, evs)
        time.sleep(1.5)  # deja que appendfsync everysec haga su trabajo

        antes_a, antes_n = foto(ra), foto(rn)
        print("\nReiniciando ambos contenedores…")
        docker("restart", AOF[0])
        docker("restart", NOPERSIST[0])
        ra = esperar("localhost", AOF[1])
        rn = esperar("localhost", NOPERSIST[1])
        despues_a, despues_n = foto(ra), foto(rn)

        filas = [
            ("stream (XLEN)", antes_a["stream"], despues_a["stream"],
             antes_n["stream"], despues_n["stream"]),
            ("grupo ingesta", int(bool(antes_a["grupos"])), int(bool(despues_a["grupos"])),
             int(bool(antes_n["grupos"])), int(bool(despues_n["grupos"]))),
            ("pendientes", sum(antes_a["grupos"].values()), sum(despues_a["grupos"].values()),
             sum(antes_n["grupos"].values()), sum(despues_n["grupos"].values())),
            ("ultimo:CC-001", int(antes_a["ultimo"]), int(despues_a["ultimo"]),
             int(antes_n["ultimo"]), int(despues_n["ultimo"])),
            ("ventana:CC-001", antes_a["ventana"], despues_a["ventana"],
             antes_n["ventana"], despues_n["ventana"]),
            ("ranking:flujo", antes_a["ranking"], despues_a["ranking"],
             antes_n["ranking"], despues_n["ranking"]),
        ]

        print(f"\n{'estructura':<18}{'AOF antes':>10}{'AOF después':>12}"
              f"{'sin pers. antes':>18}{'sin pers. después':>19}")
        for nombre, aa, ad, na, nd in filas:
            print(f"{nombre:<18}{aa:>10}{ad:>12}{na:>18}{nd:>19}")

        print("\nQué sobrevivió")
        for nombre, aa, ad, na, nd in filas:
            print(f"  {nombre:<18} AOF: {'SÍ' if ad else 'NO':<3}   "
                  f"sin persistencia: {'SÍ' if nd else 'NO'}")
            if nombre == "stream (XLEN)" and nd == 0:
                print("    (sin persistencia no hay stream: la hora en vivo se perdió)")

        print("\nVentana de pérdida")
        print("  AOF everysec: se pierde como máximo ~1 s de escrituras si el")
        print("  proceso muere (lo que alcanzó a estar en el buffer del sistema")
        print("  operativo). El stream, los grupos, los pendientes, ultimo:*,")
        print("  ranking y ventanas sobreviven al reinicio.")
        print("  Sin persistencia: se pierde TODO lo que estaba en memoria: el")
        print("  stream, los grupos y sus pendientes, el tablero completo. La")
        print("  historia ya escrita en HBase sigue a salvo porque vive en disco.")
        print("  En un sistema real aceptaría perder a lo sumo 1 s de eventos no")
        print("  confirmados: appendfsync everysec. Con AOF always la pérdida es")
        print("  cero pero cada XADD paga un fsync; con RDB se perdería el")
        print("  intervalo entre snapshots (minutos), inaceptable para 'ahora'.")
    finally:
        for nombre, _p in (AOF, NOPERSIST):
            docker("rm", "-f", nombre, check=False)


if __name__ == "__main__":
    main()
