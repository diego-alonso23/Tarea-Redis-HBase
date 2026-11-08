"""P3 — Un rango en HBase: scan por rango vs recorrido completo + filtro.

Lecturas de CC-001 el 04-nov entre 08:00 (incluido) y 10:00 (excluido):
cuántas son y su promedio por cuarto de hora.

  a) scan por rango: la row key `CC-001#<iso>` permite acotar start/stop y
     tocar una sola región (la estación está pre-dividida).
  b) recorrido de la tabla entera con PrefixFilter: HBase igual lee todas las
     regiones y descarta; solo se ahorra el transporte.

    python3 src/p3_rango.py
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime

from comun import FAMILIA, TABLA_LECTURAS, hbase_conexion
from util_medicion import medir, tabla_tiempos

INI = b"CC-001#2026-11-04T08:00:00.000-03:00"
FIN = b"CC-001#2026-11-04T10:00:00.000-03:00"
COL_VALOR = f"{FAMILIA}:valor".encode()


def agregar(filas):
    """filas: iterable de (clave, valor). Devuelve total y promedio por cuarto."""
    total = 0
    suma = 0.0
    por_cuarto: dict[str, list[float]] = defaultdict(list)
    for clave, valor in filas:
        ts = datetime.fromisoformat(clave.decode().split("#", 1)[1])
        cuarto = f"{ts.hour:02d}:{(ts.minute // 15) * 15:02d}"
        por_cuarto[cuarto].append(valor)
        total += 1
        suma += valor
    resumen = {
        c: (len(v), sum(v) / len(v)) for c, v in sorted(por_cuarto.items())
    }
    return total, (suma / total if total else float("nan")), resumen


def scan_rango(tabla):
    filas = (
        (clave, float(datos[COL_VALOR]))
        for clave, datos in tabla.scan(
            row_start=INI, row_stop=FIN, columns=[COL_VALOR]
        )
    )
    return agregar(filas)


def scan_completo(tabla):
    def filas():
        for clave, datos in tabla.scan(
            filter=b"PrefixFilter('CC-001#')", columns=[COL_VALOR]
        ):
            if INI <= clave < FIN:
                yield clave, float(datos[COL_VALOR])
    return agregar(filas())


def main() -> None:
    conn = hbase_conexion()
    tabla = conn.table(TABLA_LECTURAS)

    print("== P3 · Rango en HBase (CC-001, 2026-11-04 08:00–10:00) ==\n")
    (total, promedio, por_cuarto), med_rango, t_rango = medir(lambda: scan_rango(tabla))
    (total2, promedio2, por_cuarto2), med_full, t_full = medir(lambda: scan_completo(tabla))

    print(f"Lecturas en el rango: {total:,}")
    print(f"Promedio del rango   : {promedio:.2f}\n")
    print(f"{'cuarto':<8}{'lecturas':>10}{'promedio':>12}")
    for cuarto, (n, prom) in por_cuarto.items():
        print(f"{cuarto:<8}{n:>10}{prom:>12.2f}")

    print()
    print(tabla_tiempos("scan por rango", med_rango, t_rango))
    print(tabla_tiempos("tabla completa + filtro", med_full, t_full))
    print(f"\nEl recorrido completo es {med_full / med_rango:.1f}× más lento.")
    print(f"Ambos devuelven lo mismo: {total == total2 and abs(promedio - promedio2) < 1e-9}")

    print("\nCon 5 años en vez de 5 días (×365):")
    print(f"  el scan por rango seguiría tocando una región y un rango de ~2 h")
    print(f"  (tiempo ≈ constante), mientras el recorrido completo crecería ~365× "
          f"(≈ {med_full * 365 / 1000:.0f} s por corrida).")
    print("  Por eso la row key de `lecturas` empieza por estación: el rango es")
    print("  O(lecturas del rango) y no O(tabla).")

    conn.close()


if __name__ == "__main__":
    main()
