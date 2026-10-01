#!/usr/bin/env bash
# One command: .env (optional) -> virtual env -> dependencies -> replay data (first run only) -> server.
set -e
cd "$(dirname "$0")"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
if [ ! -d .venv ]; then python3 -m venv .venv; fi
source .venv/bin/activate
pip install -q -r requirements.txt
if [ ! -f data/aug2024.json ]; then python -m backend.fetch_replay; fi
exec python -m uvicorn backend.main:app --host 127.0.0.1 --port "${PORT:-8000}"