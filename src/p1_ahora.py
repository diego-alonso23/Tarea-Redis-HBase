"""P1 — "Ahora": Redis contra HBase.

Responde, al terminar la hora en vivo:
  * el último valor de las 16 estaciones;
  * las 5 estaciones de flujo vehicular con más flujo.

Desde Redis: `ultimo:<est>` (hash) y `ranking:flujo` (zset).
Desde HBase: la clave es `estacion#timestamp`, así que "lo último" no sale
gratis. Se resuelve con un scan **invertido** por estación (`reverse=True`,
`limit=1`): 16 scans cortos que caen cada uno en una región.

Además identifica la estación cuya respuesta cambia si se toma el último por
medición o el último por llegada (para eso sí hay que recorrer la tabla una vez
y mirar el timestamp de escritura de cada celda).

    python3 src/p1_ahora.py
"""
from __future__ import annotations

from datetime import datetime, timezone, timedelta

from comun import (
    ESTACIONES_FLUJO,
    ESTACIONES_ORDENADAS,
    FAMILIA,
    TABLA_LECTURAS,
    hbase_conexion,
    redis_conexion,
)
from util_medicion import fmt_ms, medir, tabla_tiempos

CHILE = timezone(timedelta(hours=-3))


def ms_a_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=CHILE).isoformat(timespec="milliseconds")


# --------------------------------------------------------------------------- Redis
def leer_redis(r):
    pipe = r.pipeline(transaction=False)
    for est in ESTACIONES_ORDENADAS:
        pipe.hgetall(f"ultimo:{est}")
    hashes = pipe.execute()
    ultimos = {
        est: {"valor": float(h["valor"]), "ts": ms_a_iso(int(h["ts"])), "tipo": h["tipo"]}
        for est, h in zip(ESTACIONES_ORDENADAS, hashes) if h
    }
    top = [(est, float(valor)) for est, valor in r.zrevrange("ranking:flujo", 0, 4, withscores=True)]
    return ultimos, top


def cronometrar_redis(r):
    def _q():
        return leer_redis(r)
    return medir(_q)


# --------------------------------------------------------------------------- HBase
def ultimo_hbase(tabla, est: str):
    """Scan invertido: la fila más grande de la estación es la última medición."""
    inicio = f"{est}#\xff".encode()
    fin = f"{est}#".encode()
    for clave, datos in tabla.scan(
        row_start=inicio, row_stop=fin, reverse=True, limit=1,
        columns=[f"{FAMILIA}:tipo".encode(), f"{FAMILIA}:valor".encode()],
    ):
        ts = clave.decode().split("#", 1)[1]
        return {
            "valor": float(datos[f"{FAMILIA}:valor".encode()]),
            "tipo": datos[f"{FAMILIA}:tipo".encode()].decode(),
            "ts": ts,
        }
    return None


def leer_hbase(tabla):
    ultimos = {}
    for est in ESTACIONES_ORDENADAS:
        u = ultimo_hbase(tabla, est)
        if u:
            ultimos[est] = u
    top = sorted(
        ((est, u["valor"]) for est, u in ultimos.items()
         if u["tipo"] == "flujo_vehicular"),
        key=lambda par: par[1], reverse=True,
    )[:5]
    return ultimos, top


def cronometrar_hbase(tabla):
    def _q():
        return leer_hbase(tabla)
    return medir(_q)


# ------------------------------------------------- último por medición vs llegada
def analizar_llegada(tabla):
    """Una pasada completa: para cada estación, fila con mayor ts de medición
    (lo que devuelve P1) vs fila con mayor timestamp de escritura en HBase
    (lo que devolvería 'el último que llegó')."""
    por_medicion: dict[str, tuple[str, float, int]] = {}
    por_llegada: dict[str, tuple[str, float, int]] = {}
    for clave, datos in tabla.scan(
        columns=[f"{FAMILIA}:valor".encode()], include_timestamp=True
    ):
        est, ts = clave.decode().split("#", 1)
        valor, escrito = datos[f"{FAMILIA}:valor".encode()]
        valor = float(valor)
        if est not in por_medicion or ts > por_medicion[est][0]:
            por_medicion[est] = (ts, valor, escrito)
        if est not in por_llegada or escrito > por_llegada[est][2]:
            por_llegada[est] = (ts, valor, escrito)
    return por_medicion, por_llegada


def main() -> None:
    r = redis_conexion()
    conn = hbase_conexion()
    tabla = conn.table(TABLA_LECTURAS)

    ultimos_r, top_r = leer_redis(r)
    ultimos_h, top_h = leer_hbase(tabla)

    print("== P1 · Ahora: Redis vs HBase ==\n")
    print(f"{'estación':<10}{'tipo':<26}{'Redis':>10}{'HBase':>10}   último por medición")
    for est in ESTACIONES_ORDENADAS:
        a = ultimos_r.get(est)
        b = ultimos_h.get(est)
        if not a or not b:
            print(f"{est:<10}{'FALTA':<26}")
            continue
        print(f"{est:<10}{a['tipo']:<26}{a['valor']:>10.1f}{b['valor']:>10.1f}   {a['ts']}")

    print("\nTop 5 flujo (Redis ranking:flujo vs HBase último por medición)")
    en_hbase = dict(top_h)
    for i, (est, valor) in enumerate(top_r, 1):
        print(f"  {i}. {est:<9}{valor:>8.1f}   HBase: {en_hbase.get(est, float('nan')):>8.1f}")

    print()
    _, med_r, t_r = cronometrar_redis(r)
    _, med_h, t_h = cronometrar_hbase(tabla)
    print(tabla_tiempos("Redis (16 HGETALL + ZREVRANGE)", med_r, t_r))
    print(tabla_tiempos("HBase (16 scans invertidos)", med_h, t_h))
    print(f"\nHBase es {med_h / med_r:.0f}× más lento para la misma pregunta.")

    print("\nÚltimo por medición vs último por llegada (una pasada completa)")
    por_medicion, por_llegada = analizar_llegada(tabla)
    distintas = []
    for est in ESTACIONES_ORDENADAS:
        m = por_medicion[est]
        l = por_llegada[est]
        if m[0] != l[0]:
            distintas.append((est, m, l))
    if not distintas:
        print("  No hubo diferencias en esta corrida.")
    for est, m, l in distintas:
        print(f"  {est}: por medición → {m[1]:.1f} en {m[0]}")
        print(f"            por llegada  → {l[1]:.1f} en {l[0]}  "
              f"(escrito {ms_a_iso(l[2])})")
    print(f"\n  {len(distintas)} estación(es) cambian de respuesta según el reloj elegido.")
    print("  Un dashboard usa Redis: responde en microsegundos y 'ahora' es su único")
    print("  contrato. HBase sirve para auditar qué lectura llegó última, no para el tablero.")

    conn.close()


if __name__ == "__main__":
    main()
