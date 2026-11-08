"""Constantes, conexiones y decisiones de diseño compartidas de la Tarea 2.

Todo lo que define "dónde vive cada dato" está acá para que los scripts no
repitan (y no se desincronicen) las mismas decisiones:

  * `lecturas`      row key = estacion#timestamp_de_medicion
  * `lecturas_tipo` row key = tipo#timestamp_de_medicion#estacion
  * stream          sensores:t2 (Redis)
  * tablero         ultimo:<est>, ranking:flujo, ventana:<est>, tipo:<tipo>

Los timestamps se escriben siempre en ISO-8601 con milisegundos y offset fijo
-03:00 (hora de Chile). Así el orden lexicográfico de la row key coincide con el
orden temporal y no se mezcla UTC con hora local.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
# Consola Windows en cp1252: evitar UnicodeEncodeError con ·, →, ×.
for _flujo in (sys.stdout, sys.stderr):
    try:
        _flujo.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
# El generador vive en scripts/ y es la única fuente de verdad de los datos.
sys.path.insert(0, str(RAIZ / "scripts"))
from generador import ESTACIONES, RANGOS, eventos  # noqa: E402

# ---------------------------------------------------------------------------
# Parámetros de la tarea (PDF, sección 2)
# ---------------------------------------------------------------------------
T0 = datetime(2026, 11, 2, 0, 0, tzinfo=timezone(timedelta(hours=-3)))
SEMILLA = 2026                 # ojo: el default del generador del Lab 06 es 2509
PASO = 0.5                     # segundos de reloj simulado entre eventos
N_HIST = 864_000               # 5 días × 172.800
N_VIVO = 7_200                 # 1 hora
RITMO_VIVO = 0.025             # 25 ms reales entre eventos = 20× el ritmo real

STREAM = "sensores:t2"
GRUPO_INGESTA = "ingesta"
GRUPO_TABLERO = "tablero"
CONSUMIDOR_INGESTA = "ingesta-1"
CONSUMIDOR_TABLERO = "tablero-1"

TABLA_LECTURAS = "lecturas"
TABLA_TIPO = "lecturas_tipo"
FAMILIA = "d"
VERSIONES = 3                  # para poder mostrar versiones en el experimento D.1

VENTANA_MS = 10 * 60 * 1000    # ventana del tablero: 10 minutos de medición

# Números de control del enunciado (para verificar.py)
CONTROL = {
    "historico": {"eventos": 864_000, "repetidos": 17_090, "claves": 846_906},
    "vivo": {"eventos": 7_200, "repetidos": 153, "claves": 7_047},
    "total": {"eventos": 871_200, "repetidos": 17_243, "claves": 853_953},
}

TIPOS = sorted({t for _, t in ESTACIONES})
ESTACIONES_ORDENADAS = sorted({e for e, _ in ESTACIONES})
ESTACIONES_FLUJO = sorted({e for e, t in ESTACIONES if t == "flujo_vehicular"})

COMUNA = {
    "CC": "Concepción",
    "TAL": "Talcahuano",
    "HUA": "Hualpén",
    "SPP": "San Pedro de la Paz",
    "CHI": "Chiguayante",
    "PEN": "Penco",
    "COR": "Coronel",
}


# ---------------------------------------------------------------------------
# Utilidades de tiempo y row keys
# ---------------------------------------------------------------------------
def iso(dt: datetime) -> str:
    """ISO-8601 con milisegundos y el mismo offset -03:00 para todos."""
    return dt.isoformat(timespec="milliseconds")


def ts_medicion(ev: dict) -> datetime:
    return datetime.fromisoformat(ev["timestamp"])


def ts_ms(ev: dict) -> int:
    return int(ts_medicion(ev).timestamp() * 1000)


def row_lecturas(ev: dict) -> bytes:
    """Estación primero: un scan por estación+rango cae en una sola región."""
    return f"{ev['estacion_id']}#{iso(ts_medicion(ev))}".encode()


def row_lecturas_tipo(ev: dict) -> bytes:
    """Tipo primero: P4b resuelve todas las estaciones con un solo scan."""
    return f"{ev['tipo_sensor']}#{iso(ts_medicion(ev))}#{ev['estacion_id']}".encode()


def comuna(estacion_id: str) -> str:
    return COMUNA[estacion_id.split("-")[0]]


# ---------------------------------------------------------------------------
# Splits (regiones iniciales)
# ---------------------------------------------------------------------------
def splits_lecturas() -> list[bytes]:
    """Una región por estación (16).

    Como la row key empieza por `estacion#`, cada estación queda en una región
    contigua: el scan de P3 (CC-001 entre 08:00 y 10:00) toca exactamente una.
    """
    return [e.encode() for e in ESTACIONES_ORDENADAS[1:]]


def splits_lecturas_tipo() -> list[bytes]:
    """Una región por (tipo, día): 3 tipos × 5 días ≈ 13 regiones.

    La row key empieza por `tipo#`, así que el scan de P4b (calidad_aire entre
    07:00 y 09:00 del 05-nov) toca una sola región en vez de recorrer el tipo
    completo. Los cortes caen en la medianoche de cada día (offset -03:00).
    """
    cortes: list[bytes] = []
    for tipo in TIPOS:                       # ordenados: c < f < o
        for dia in range(1, 5):              # 4 cortes → 5 regiones por tipo
            cortes.append(f"{tipo}#{iso(T0 + timedelta(days=dia))}".encode())
    return cortes


# ---------------------------------------------------------------------------
# Conexiones
# ---------------------------------------------------------------------------
def redis_conexion():
    import redis

    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    r = redis.Redis.from_url(url, decode_responses=True)
    r.ping()
    return r


def hbase_conexion():
    import happybase

    host = os.environ.get("HBASE_HOST", "localhost")
    port = int(os.environ.get("HBASE_PORT", "9090"))
    c = happybase.Connection(host, port=port, timeout=60_000)
    c.open()
    return c


def hbase_shell(script: str) -> str:
    """Ejecuta HBase shell dentro del contenedor (Thrift1 no expone SPLITS).

    Se usa solo para crear/borrar tablas. En una máquina limpia basta con
    `docker compose up -d --wait`; el servicio se llama `hbase` en el compose.
    """
    proc = subprocess.run(
        ["docker", "compose", "exec", "-T", "hbase", "hbase", "shell", "-n"],
        input=script,
        text=True,
        capture_output=True,
        cwd=str(RAIZ),
    )
    salida = proc.stdout + proc.stderr
    if proc.returncode != 0 or "ERROR" in salida:
        raise RuntimeError(f"HBase shell falló:\n{salida}")
    return salida


def crear_tabla(nombre: str, splits: list[bytes], reset: bool) -> None:
    if reset:
        hbase_shell(
            f"if exists '{nombre}'\n"
            f"  disable '{nombre}'\n"
            f"  drop '{nombre}'\n"
            f"end\n"
        )
    lista = ", ".join(f"'{s.decode()}'" for s in splits)
    hbase_shell(
        f"create '{nombre}', "
        f"{{NAME => '{FAMILIA}', VERSIONS => {VERSIONES}}}, "
        f"{{SPLITS => [{lista}]}}\n"
    )
