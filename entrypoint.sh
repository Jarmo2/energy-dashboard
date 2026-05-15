#!/bin/sh
echo "=== Starte API ==="
exec uvicorn api:app --host 0.0.0.0 --port 8000
