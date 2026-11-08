#!/usr/bin/env python3
"""Valida los números de control de la Tarea 2 (semilla 2026)."""
import sys
from datetime import datetime, timedelta, timezone
from itertools import islice
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from generador import eventos  # noqa: E402

T0 = datetime(2026, 11, 2, 0, 0, tzinfo=timezone(timedelta(hours=-3)))
SEMILLA = 2026          # el default del generador es 2509
N_HIST, N_VIVO = 864_000, 7_200

ESPERADO = {
    "historico": {"total": 864_000, "repetidos": 17_090, "claves": 846_906},
    "en_vivo":   {"total": 7_200,   "repetidos": 153,    "claves": 7_047},
    "total":     {"total": 871_200, "repetidos": 17_243, "claves": 853_953},
}


def analizar(evs):
    """Devuelve total, repetidos exactos, claves distintas y choques de clave."""
    vistos = {}  # (estacion, timestamp) -> set de (tipo, valor) distintos
    total = repetidos = 0
    for ev in evs:
        clave = (ev["estacion_id"], ev["timestamp"])
        contenido = (ev["tipo_sensor"], ev["valor"])
        total += 1
        s = vistos.setdefault(clave, set())
        if contenido in s:
            repetidos += 1
        else:
            s.add(contenido)
    choques = sum(1 for s in vistos.values() if len(s) > 1)
    return {"total": total, "repetidos": repetidos,
            "claves": len(vistos), "choques": choques}


def main():
    gen = eventos(T0, semilla=SEMILLA)
    hist = list(islice(gen, N_HIST))
    vivo = list(islice(gen, N_VIVO))   # continúa el mismo generador

    res = {"historico": analizar(hist),
           "en_vivo": analizar(vivo),
           "total": analizar(hist + vivo)}  # claves sobre ambos tramos juntos

    ok = True
    print(f"{'tramo':<11}{'métrica':<11}{'obtenido':>10}{'esperado':>10}")
    for tramo, esp in ESPERADO.items():
        for m, v in esp.items():
            got = res[tramo][m]
            ok &= got == v
            print(f"{tramo:<11}{m:<11}{got:>10,}{v:>10,}  {'OK' if got == v else 'MAL'}")

    t = res["total"]
    print(f"\nTotal - claves = {t['total'] - t['claves']:,} "
          f"= {t['repetidos']:,} repetidos + "
          f"{t['total'] - t['claves'] - t['repetidos']} choques de clave")
    print(f"Claves con valores distintos: {t['choques']} (esperado: 4)")
    ok &= t["choques"] == 4

    print("\nRESULTADO:", "TODO CALZA" if ok else "NO CALZA")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()