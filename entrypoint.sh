#!/bin/sh
# Beim Container-Start: aktuelle Daten holen, dann API starten.

echo "=== Lade aktuelle Energiedaten ==="
python etl.py --weeks 1

echo "=== Starte API ==="
uvicorn api:app --host 0.0.0.0 --port 8000
