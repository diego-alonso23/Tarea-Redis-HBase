#!/usr/bin/env bash
# Tarea 2 — Redis + HBase: corre TODO de punta a punta.
#
#   ./run.sh              # desde cero: recrea tablas, carga 864.000, corre la hora en vivo y verifica
#   KEEP=1 ./run.sh       # reutiliza lo ya cargado en HBase (desarrollo)
#   LIMITE=20000 ./run.sh # corrida corta de humo (no calza los números de control)
#
# Requisitos: Docker con ~4 GB de RAM y python3 (o python) en el PATH.
set -euo pipefail
cd "$(dirname "$0")"

LIMITE="${LIMITE:-}"
KEEP="${KEEP:-0}"
VENV="${VENV:-.venv}"

echo "== 1/6 Contenedores =="
docker compose up -d --wait

echo "== 2/6 Entorno Python =="
PY="${PYTHON:-python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python
[ -d "$VENV" ] || "$PY" -m venv "$VENV"
if [ -f "$VENV/bin/activate" ]; then
  # shellcheck disable=SC1091
  source "$VENV/bin/activate"
else
  # shellcheck disable=SC1091
  source "$VENV/Scripts/activate"
fi
python -m pip install -q -r requirements.txt
PY=python

echo "== 3/6 Estado limpio =="
# Redis se vacía para que los grupos y contadores partan de cero.
docker compose exec -T redis redis-cli FLUSHALL >/dev/null

echo "== 4/6 Pipeline =="
ARGS_CARGA=()
if [ "$KEEP" = "1" ]; then
  ARGS_CARGA+=(--keep)
fi
if [ -n "$LIMITE" ]; then
  ARGS_CARGA+=(--limite "$LIMITE")
fi
python src/carga_historica.py "${ARGS_CARGA[@]}"

mkdir -p logs
# Los consumidores primero: crean sus grupos. El productor emite los 7.200 en vivo.
python src/consumidor_ingesta.py >logs/ingesta.log 2>&1 &
PID_INGESTA=$!
python src/consumidor_tablero.py >logs/tablero.log 2>&1 &
PID_TABLERO=$!

ARGS_PRODUCTOR=()
if [ -n "$LIMITE" ]; then
  ARGS_PRODUCTOR+=(--limite "$LIMITE")
fi
python src/productor.py "${ARGS_PRODUCTOR[@]}"

wait "$PID_INGESTA"
wait "$PID_TABLERO"
echo "-- ingesta --"; tail -n 3 logs/ingesta.log
echo "-- tablero --"; tail -n 3 logs/tablero.log

echo "== 5/6 Verificación =="
python src/verificar.py

echo "== 6/6 Consultas (Parte C) =="
if [ "${SIN_CONSULTAS:-0}" = "1" ]; then
  echo "omitidas (SIN_CONSULTAS=1)"
else
  for consulta in p1_ahora p2_ventana p3_rango p4_patrones p5_hotspotting p6_propia; do
    echo "-- $consulta --"
    python "src/$consulta.py"
  done
fi
