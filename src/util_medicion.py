"""Medición de tiempos: mediana de 5 corridas, descartando la primera.

El enunciado pide explícitamente "la mediana de 5 corridas, descartando la
primera: calienta cachés". `medir` hace 1 corrida de calentamiento + 5 medidas y
devuelve el resultado de la última corrida y la mediana en milisegundos.
"""
from __future__ import annotations

import statistics
import time
from typing import Callable, Tuple


def medir(fn: Callable, corridas: int = 5, calentar: bool = True) -> Tuple[object, float, list[float]]:
    if calentar:
        fn()  # primera corrida: no se mide
    tiempos: list[float] = []
    resultado = None
    for _ in range(corridas):
        t0 = time.perf_counter()
        resultado = fn()
        tiempos.append((time.perf_counter() - t0) * 1000)  # ms
    return resultado, statistics.median(tiempos), tiempos


def fmt_ms(x: float) -> str:
    if x < 1:
        return f"{x * 1000:.0f} µs"
    if x < 1000:
        return f"{x:.1f} ms"
    return f"{x / 1000:.2f} s"


def tabla_tiempos(nombre: str, mediana: float, tiempos: list[float]) -> str:
    detalle = " · ".join(f"{t:.2f}" for t in tiempos)
    return f"{nombre:<28}{fmt_ms(mediana):>10}   (corridas ms: {detalle})"
