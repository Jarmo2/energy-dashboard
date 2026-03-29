# Hannover Energy Dashboard

Deutschlands Energiemix, visualisiert. Öffentliche SMARD-Daten der Bundesnetzagentur,
aufbereitet über eine Python-ETL-Pipeline und bereitgestellt als REST-API mit
interaktivem Dashboard.

**Live:** [jarmok.de/energy](https://jarmok.de) (coming soon)

## Datenquelle

[SMARD.de](https://smard.de) — Strommarktdaten der Bundesnetzagentur (CC BY 4.0).

## Quickstart

```bash
# Abhängigkeiten installieren
pip install -r requirements.txt

# Daten laden (letzte 4 Wochen)
python etl.py --weeks 4

# API starten
uvicorn api:app --reload --port 8000

# Dashboard öffnen
# http://localhost:8000
```

### Mit Docker

```bash
docker compose up --build
# http://localhost:8000
```

## API-Endpunkte

| Endpunkt | Beschreibung |
|---|---|
| `GET /energy-mix?days=7` | Prozentuale Verteilung aller Energieträger |
| `GET /timeseries?filter_name=Photovoltaik&days=7` | Stündliche Werte für einen Träger |
| `GET /summary?days=7` | Gesamtübersicht mit Erneuerbar-Anteil |
| `GET /filters` | Verfügbare Energieträger |
| `GET /docs` | Swagger API-Dokumentation |

## Projektstruktur

```
├── smard_client.py       # SMARD API Client (Pydantic Models, HTTP)
├── etl.py                # ETL Pipeline: API → SQLite
├── api.py                # FastAPI Endpunkte + statische Seiten
├── static/
│   ├── index.html        # Dashboard Frontend (Chart.js)
│   ├── about.html        # Über mich
│   ├── impressum.html    # Impressum
│   └── datenschutz.html  # Datenschutzerklärung
├── tests/
│   ├── test_etl.py       # Unit Tests (ETL + SMARD Client)
│   └── test_api.py       # API Tests (FastAPI Endpunkte)
├── Dockerfile            # Container-Definition
├── docker-compose.yml    # Lokale Entwicklung mit Docker
├── entrypoint.sh         # Container-Startscript
├── requirements.txt      # Python-Abhängigkeiten
└── energy.db             # SQLite (wird per ETL erzeugt, nicht im Repo)
```

## Tech Stack

| Komponente | Technologie |
|---|---|
| ETL | Python, pandas, pydantic, requests |
| API | FastAPI, uvicorn |
| Datenbank | SQLite |
| Frontend | HTML, Chart.js |
| Container | Docker, Docker Compose |
| Tests | pytest, httpx |

## Tests

```bash
# Alle Tests
pytest tests.py test_api.py -v

# Nur ETL/Client Tests
pytest tests.py -v

# Nur API Tests
pytest test_api.py -v
```

## Lizenz

Code: MIT. Daten: CC BY 4.0 (Bundesnetzagentur | SMARD.de).

## Autor

Jarmo Kruse — [jarmok.de](https://jarmok.de) · [GitHub](https://github.com/Jarmo2) · [LinkedIn](https://linkedin.com/in/jarmo-kruse)
