"""P5 — Hotspotting y salting en HBase.

Escribe la hora en vivo (los 7.200 eventos del stream) en tres tablas con la
misma información y distinta row key:

  hv_estacion  estacion#ts            pre-dividida por estación (16 regiones)
  hv_ts        ts#estacion            pre-dividida por día (5 regiones)
  hv_salt      salt#ts#estacion       salt = crc32(estacion) mod 4 (4 regiones)

Mide cuántas escrituras recibió cada región con las métricas JMX del
RegionServer (sub=Regions) y después cronometra la misma pregunta con cada
diseño: todas las lecturas de todas las estaciones entre 00:10 y 00:20 del
07-nov (16 rangos, 1 rango, 4 rangos).

    python3 src/p5_hotspotting.py
"""
from __future__ import annotations

import itertools
import json
import re
import subprocess
import time
import zlib

from comun import (
    ESTACIONES_ORDENADAS,
    FAMILIA,
    N_HIST,
    N_VIVO,
    PASO,
    RAIZ,
    SEMILLA,
    STREAM,
    T0,
    crear_tabla,
    eventos,
    hbase_conexion,
    hbase_shell,
    iso,
    redis_conexion,
    splits_lecturas,
)
from util_medicion import medir, tabla_tiempos

TABLAS = ("hv_estacion", "hv_ts", "hv_salt")
COL_VALOR = f"{FAMILIA}:valor".encode()
INI = "2026-11-07T00:10:00.000-03:00"
FIN = "2026-11-07T00:20:00.000-03:00"
ESTACIONES = ESTACIONES_ORDENADAS


def splits_ts() -> list[bytes]:
    from datetime import timedelta

    return [iso(T0 + timedelta(days=d)).encode() for d in range(1, 5)]


def salt(est: str) -> str:
    return str(zlib.crc32(est.encode()) % 4)


def claves(ev):
    est, ts = ev["estacion_id"], ev["timestamp"]
    return {
        "hv_estacion": f"{est}#{ts}".encode(),
        "hv_ts": f"{ts}#{est}".encode(),
        "hv_salt": f"{salt(est)}#{ts}#{est}".encode(),
    }


def eventos_vivo() -> list[dict]:
    """La hora en vivo: preferentemente desde el stream; si no, del generador."""
    try:
        r = redis_conexion()
        crudos = r.xrange(STREAM)
        if len(crudos) >= N_VIVO:
            print(f"  leyendo {len(crudos):,} eventos del stream {STREAM}")
            return [ev for _id, ev in crudos[-N_VIVO:]]
    except Exception:
        pass
    print("  stream incompleto: regenerando la hora en vivo")
    gen = eventos(T0, semilla=SEMILLA, paso=PASO)
    return list(itertools.islice(gen, N_HIST, N_HIST + N_VIVO))


def jmx_regiones() -> dict:
    salida = subprocess.run(
        ["docker", "compose", "exec", "-T", "hbase", "bash", "-c",
         "curl -s 'http://localhost:16030/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions'"],
        capture_output=True, text=True, cwd=str(RAIZ),
    )
    return json.loads(salida.stdout)["beans"][0]


def regiones(tabla: str) -> list[dict]:
    """list_regions da start/end key; el nombre trae el hash que usa JMX."""
    salida = hbase_shell(f"list_regions '{tabla}'\n")
    out = []
    for linea in salida.splitlines():
        if "|" not in linea or "REGION_NAME" in linea or "---" in linea:
            continue
        partes = [p.strip() for p in linea.split("|")]
        if len(partes) < 5:
            continue
        region = partes[1]
        m = re.search(r"([0-9a-f]{32})", region)
        if not m:
            continue
        out.append({
            "hash": m.group(1),
            "inicio": partes[2],
            "fin": partes[3],
        })
    return out


def escrituras_por_region(tabla: str, bean: dict) -> dict[str, int]:
    prefijo = f"Namespace_default_table_{tabla}_region_"
    salida = {}
    for clave, valor in bean.items():
        if clave.startswith(prefijo) and clave.endswith("_metric_writeRequestCount"):
            h = clave[len(prefijo):-len("_metric_writeRequestCount")]
            salida[h] = int(valor)
    return salida


def cargar(tablas, evs) -> float:
    t0 = time.perf_counter()
    for nombre, tabla in tablas.items():
        with tabla.batch(batch_size=1_000) as b:
            for ev in evs:
                b.put(claves(ev)[nombre], {f"{FAMILIA}:valor": str(ev["valor"]).encode()})
    return time.perf_counter() - t0


def contar(tabla, prefijos, nombre):
    n = 0
    for p in prefijos:
        for _k, _v in tabla.scan(
            row_start=f"{p}{INI}".encode(),
            row_stop=f"{p}{FIN}".encode(),
            columns=[COL_VALOR],
        ):
            n += 1
    return n


def main() -> None:
    print("== P5 · Hotspotting y salting ==\n")
    print("Creando tablas pre-divididas…")
    crear_tabla("hv_estacion", splits_lecturas(), reset=True)
    crear_tabla("hv_ts", splits_ts(), reset=True)
    crear_tabla("hv_salt", [b"1", b"2", b"3"], reset=True)

    conn = hbase_conexion()
    tablas = {n: conn.table(n) for n in TABLAS}
    evs = eventos_vivo()
    print(f"Escribiendo {len(evs):,} eventos en 3 tablas…")
    seg = cargar(tablas, evs)
    print(f"  listo en {seg:.1f} s ({len(evs) / seg:,.0f} eventos/s, "
          f"{3 * len(evs) / seg:,.0f} puts/s)\n")

    print("Esperando ~12 s a que JMX publique los contadores…")
    time.sleep(12)
    bean = jmx_regiones()

    resumen = {}
    for tabla in TABLAS:
        regs = regiones(tabla)
        conteo = escrituras_por_region(tabla, bean)
        filas = [(r["inicio"] or "(inicio)", r["fin"] or "(fin)",
                  conteo.get(r["hash"], 0)) for r in regs]
        resumen[tabla] = filas
        total = sum(c for _i, _f, c in filas)
        print(f"\n{tabla}: {len(filas)} regiones · {total:,} escrituras")
        print(f"  {'inicio':<36}{'fin':<36}{'escrituras':>12}")
        for ini, fin, c in filas:
            print(f"  {ini[:35]:<36}{fin[:35]:<36}{c:>12,}")
        maximas = max(c for _i, _f, c in filas)
        minimas = min(c for _i, _f, c in filas)
        if minimas:
            print(f"  max/min = {maximas / minimas:.2f}× "
                  f"(1 = perfectamente balanceada)")
        else:
            print(f"  max/min = ∞ (máx {maximas:,} en una región, "
                  f"{sum(1 for *_x, c in filas if c == 0)} regiones vacías)")

    print("\nTiempos de la consulta 00:10–00:20 del 07-nov")
    (n1, m1, t1) = medir(lambda: contar(tablas["hv_estacion"],
                                        [f"{e}#" for e in ESTACIONES], "16 rangos"))
    (n2, m2, t2) = medir(lambda: contar(tablas["hv_ts"], [""], "1 rango"))
    (n3, m3, t3) = medir(lambda: contar(tablas["hv_salt"], [f"{s}#" for s in "0123"],
                                        "4 rangos"))
    print(tabla_tiempos("hv_estacion (16 rangos)", m1, t1))
    print(tabla_tiempos("hv_ts (1 rango)", m2, t2))
    print(tabla_tiempos("hv_salt (4 rangos)", m3, t3))
    print(f"\nFilas devueltas: {n1:,} · {n2:,} · {n3:,} "
          f"(iguales: {n1 == n2 == n3})")

    print("\nLectura")
    print("  La clave est#ts reparte las escrituras entre las 16 regiones y deja")
    print("  la consulta por estación en 16 rangos cortos.")
    print("  La clave ts#est concentra la hora en una sola región: escribir es")
    print("  más rápido (un solo servidor) pero esa región es el hotspot y crece")
    print("  con el tiempo, no con las estaciones.")
    print("  El salt reparte las escrituras entre 4 regiones y la consulta en 4")
    print("  rangos, pero deja de ser eficiente para 'una estación en un rango'")
    print("  (hay que visitar los 4 salts).")
    print("  Elegiría est#ts para el patrón mixto del pipeline (escrituras")
    print("  continuas + consultas por estación): reparte las escrituras sin")
    print("  hotspot y mantiene P3 en una región, a cambio de 16 rangos en la")
    print("  consulta de todas las estaciones. Si esa consulta dominara y las")
    print("  escrituras fueran bajas, elegiría ts#est aceptando el hotspot;")
    print("  salt es el compromiso. Cambiaría a salt si las escrituras saturaran")
    print("  una región por concentración de estaciones.")

    conn.close()


if __name__ == "__main__":
    main()
