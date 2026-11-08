"""B.3 Verificación: ¿los números de control calzan?

Imprime:
  * eventos producidos (histórico + en vivo);
  * entradas procesadas y XPENDING de cada grupo de consumidores (debe ser 0);
  * filas reales en cada tabla de HBase (contadas con filtros, no con un SELECT *);
  * la comparación contra los números de control del enunciado y una explicación
    de cada diferencia que sí corresponde (repetidos y choques de clave).

    python3 src/verificar.py            # cuenta HBase (tarda)
    python3 src/verificar.py --rapido   # sin contar HBase (solo Redis)
"""
from __future__ import annotations

import argparse
import time

from comun import (
    CONTROL,
    GRUPO_INGESTA,
    GRUPO_TABLERO,
    STREAM,
    TABLA_LECTURAS,
    TABLA_TIPO,
    hbase_conexion,
    redis_conexion,
)

FILTRO_CONTAR = b"FirstKeyOnlyFilter() AND KeyOnlyFilter()"


def contar_filas(tabla) -> tuple[int, float]:
    t0 = time.perf_counter()
    n = 0
    for _ in tabla.scan(filter=FILTRO_CONTAR, batch_size=1_000):
        n += 1
    return n, time.perf_counter() - t0


def estado_grupo(r, grupo: str) -> dict:
    grupos = {g["name"]: g for g in r.xinfo_groups(STREAM)}
    g = grupos.get(grupo)
    if g is None:
        return {"existe": False}
    return {
        "existe": True,
        "entregadas": int(g.get("entries-read") or 0),
        "pendientes": int(g.get("pending") or 0),
        "lag": g.get("lag"),
        "ultimo_id": g.get("last-delivered-id"),
    }


def fila_ok(nombre: str, obtenido: int, esperado: int) -> bool:
    ok = obtenido == esperado
    print(f"  {nombre:<34}{obtenido:>12,}{esperado:>12,}   {'OK' if ok else 'MAL'}")
    return ok


def main() -> None:
    ap = argparse.ArgumentParser(description="Verificación de la Tarea 2")
    ap.add_argument("--rapido", action="store_true", help="no contar filas en HBase")
    args = ap.parse_args()

    r = redis_conexion()
    todo_ok = True

    print("== Verificación Tarea 2 ==")
    print(f"Stream: {STREAM} · largo actual: {r.xlen(STREAM):,}")

    meta = r.hgetall("t2:meta")
    hist_eventos = int(meta.get("historico_eventos", 0))
    vivo_eventos = int(r.get("t2:producidos") or 0)

    print("\nEventos")
    print(f"  {'origen':<34}{'obtenido':>12}{'esperado':>12}   estado")
    todo_ok &= fila_ok("histórico cargado", hist_eventos, CONTROL["historico"]["eventos"])
    todo_ok &= fila_ok("en vivo producidos", vivo_eventos, CONTROL["vivo"]["eventos"])
    todo_ok &= fila_ok("total", hist_eventos + vivo_eventos, CONTROL["total"]["eventos"])

    print("\nGrupos de consumidores")
    print(f"  {'grupo':<14}{'procesadas':>12}{'XPENDING':>10}{'lag':>8}")
    for grupo in (GRUPO_INGESTA, GRUPO_TABLERO):
        e = estado_grupo(r, grupo)
        if not e["existe"]:
            print(f"  {grupo:<14}{'NO EXISTE':>12}")
            todo_ok = False
            continue
        print(f"  {grupo:<14}{e['entregadas']:>12,}{e['pendientes']:>10}"
              f"{str(e['lag']):>8}")
        if e["pendientes"] != 0:
            print(f"    AVISO: {grupo} tiene {e['pendientes']} entradas pendientes")
            todo_ok = False
        if e["entregadas"] != CONTROL["vivo"]["eventos"]:
            print(f"    AVISO: {grupo} procesó {e['entregadas']:,} y se esperaban "
                  f"{CONTROL['vivo']['eventos']:,}")
            todo_ok = False

    if not args.rapido:
        print("\nFilas en HBase (scan con FirstKeyOnlyFilter + KeyOnlyFilter)")
        conn = hbase_conexion()
        print(f"  {'tabla':<34}{'obtenido':>12}{'esperado':>12}   estado")
        for nombre in (TABLA_LECTURAS, TABLA_TIPO):
            tabla = conn.table(nombre)
            n, seg = contar_filas(tabla)
            todo_ok &= fila_ok(nombre, n, CONTROL["total"]["claves"])
            print(f"    (conteo en {seg:.1f} s)")
        conn.close()

    total = CONTROL["total"]
    absorbidos = total["eventos"] - total["claves"]
    print("\nExplicación de la diferencia")
    if hist_eventos + vivo_eventos != total["eventos"]:
        print("  (corrida parcial: los números de control corresponden a la")
        print("   corrida completa, sin LIMITE; no se comparan)")
    print(f"  eventos - claves = {absorbidos:,} = "
          f"{total['repetidos']:,} repetidos exactos + "
          f"{absorbidos - total['repetidos']} pares con la misma clave "
          f"(estación+milisegundo) y distinto valor")
    print("  Los repetidos y los choques se absorben porque la row key sale de la")
    print("  estación y de la medición: la última escritura pisa la anterior, no")
    print("  agrega una fila nueva. Es el comportamiento esperado, no un error.")

    print("\nRESULTADO:", "TODO CALZA" if todo_ok else "NO CALZA (revisar avisos)")
    raise SystemExit(0 if todo_ok else 1)


if __name__ == "__main__":
    main()
