Hannover Energy Dashboard
FastAPI backend + Chart.js frontend, deployed on IONOS VPS via nginx.
Data source: SMARD API → ETL pipeline → SQLite.
Stack

Python 3.11, FastAPI, SQLite
Chart.js (vanilla JS, no framework)
Docker + nginx (reverse proxy on port 80 → app on 8000)
Ruff for linting and formatting (PEP8)

Commands

docker compose up --build – lokaler Start
pytest – Tests ausführen
ruff check . – Linting
ruff format . – Formatierung

Project structure

routers/ – FastAPI route definitions
db/ – database access layer (all DB logic stays here)
etl/ – SMARD API client and ETL pipeline
static/ – Chart.js frontend

Coding style

Write code at an intermediate level – readable over clever
Prefer explicit over implicit (no magic, no over-abstraction)
Add short inline comments for non-obvious logic
Avoid unnecessary design patterns if a simple function does the job
Follow PEP8, enforced via Ruff

Conventions

New endpoints always go in routers/
No direct DB access outside of db/
Keep ETL logic separate from API logic

Do not touch

nginx.conf only on explicit request