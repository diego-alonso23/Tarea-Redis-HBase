#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# 1. Contenedores
docker compose up -d --wait

# 2. Python
[ -d .venv ] || python3 -m venv .venv
source .venv/bin/activate
pip install -q -r requirements.txt

# 3. Pipeline (se irá completando en la Parte B)
# python src/carga_historica.py
# python src/productor.py &
# python src/consumidor_ingesta.py &
# python src/consumidor_tablero.py &
# wait
# python src/verificar.py