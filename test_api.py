"""
Tests für api.py — FastAPI Endpunkte.

Ausführen: pytest test_api.py -v

Die Tests erstellen eine temporäre Datenbank mit Testdaten,
damit sie unabhängig von der echten energy.db laufen.
"""

import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from api import app, DB_PATH


@pytest.fixture
def test_db(tmp_path):
    """Erstellt eine temporäre Datenbank mit Testdaten."""
    db_path = tmp_path / "test_energy.db"
    conn = sqlite3.connect(str(db_path))

    conn.executescript("""
        CREATE TABLE energy_generation (
            timestamp_ms    INTEGER NOT NULL,
            timestamp_utc   TEXT NOT NULL,
            filter_id       INTEGER NOT NULL,
            filter_name     TEXT NOT NULL,
            is_renewable    INTEGER NOT NULL,
            value_mwh       REAL,
            PRIMARY KEY (timestamp_ms, filter_id)
        );
    """)

    # Testdaten: 3 Energieträger, jeweils 3 Datenpunkte
    # Timestamps: letzte 24 Stunden (damit die Abfragen Daten finden)
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    base_ms = int(now.timestamp() * 1000)
    hour_ms = 3_600_000

    test_data = [
        # Photovoltaik (erneuerbar)
        (base_ms - 2 * hour_ms, (now - timedelta(hours=2)).isoformat(), 4068, "Photovoltaik", 1, 5000.0),
        (base_ms - 1 * hour_ms, (now - timedelta(hours=1)).isoformat(), 4068, "Photovoltaik", 1, 6000.0),
        (base_ms,               now.isoformat(),                        4068, "Photovoltaik", 1, 5500.0),
        # Wind Onshore (erneuerbar)
        (base_ms - 2 * hour_ms, (now - timedelta(hours=2)).isoformat(), 4067, "Wind Onshore", 1, 8000.0),
        (base_ms - 1 * hour_ms, (now - timedelta(hours=1)).isoformat(), 4067, "Wind Onshore", 1, 9000.0),
        (base_ms,               now.isoformat(),                        4067, "Wind Onshore", 1, 8500.0),
        # Erdgas (konventionell)
        (base_ms - 2 * hour_ms, (now - timedelta(hours=2)).isoformat(), 4071, "Erdgas", 0, 4000.0),
        (base_ms - 1 * hour_ms, (now - timedelta(hours=1)).isoformat(), 4071, "Erdgas", 0, 3500.0),
        (base_ms,               now.isoformat(),                        4071, "Erdgas", 0, 3800.0),
    ]

    conn.executemany(
        "INSERT INTO energy_generation VALUES (?, ?, ?, ?, ?, ?)",
        test_data,
    )
    conn.commit()
    conn.close()

    return db_path


@pytest.fixture
def client(test_db):
    """Test-Client mit gemocktem DB-Pfad."""
    with patch("api.DB_PATH", test_db):
        yield TestClient(app)


class TestEnergyMix:
    def test_returns_all_carriers(self, client):
        response = client.get("/energy-mix?days=7")
        assert response.status_code == 200
        data = response.json()
        assert len(data["mix"]) == 3

    def test_shares_sum_to_100(self, client):
        response = client.get("/energy-mix?days=7")
        data = response.json()
        total_share = sum(entry["share_percent"] for entry in data["mix"])
        assert abs(total_share - 100.0) < 0.5  # Rundungstoleraz

    def test_renewable_share_is_correct(self, client):
        response = client.get("/energy-mix?days=7")
        data = response.json()
        # Erneuerbar: PV (16500) + Wind (25500) = 42000
        # Konventionell: Erdgas (11300)
        # Total: 53300
        # Erneuerbar-Anteil: ~78.8%
        assert data["renewable_share_percent"] > 70

    def test_sorted_by_total_descending(self, client):
        response = client.get("/energy-mix?days=7")
        data = response.json()
        totals = [entry["total_mwh"] for entry in data["mix"]]
        assert totals == sorted(totals, reverse=True)

    def test_invalid_days(self, client):
        response = client.get("/energy-mix?days=0")
        assert response.status_code == 422

        response = client.get("/energy-mix?days=999")
        assert response.status_code == 422


class TestTimeSeries:
    def test_returns_data_points(self, client):
        response = client.get("/timeseries?filter_name=Photovoltaik&days=7")
        assert response.status_code == 200
        data = response.json()
        assert data["filter_name"] == "Photovoltaik"
        assert data["data_points"] == 3
        assert len(data["data"]) == 3

    def test_data_is_chronological(self, client):
        response = client.get("/timeseries?filter_name=Photovoltaik&days=7")
        data = response.json()
        timestamps = [p["timestamp_utc"] for p in data["data"]]
        assert timestamps == sorted(timestamps)

    def test_unknown_filter_returns_404(self, client):
        response = client.get("/timeseries?filter_name=Atomkraft&days=7")
        assert response.status_code == 404
        assert "Verfügbar" in response.json()["detail"]

    def test_missing_filter_returns_422(self, client):
        response = client.get("/timeseries?days=7")
        assert response.status_code == 422


class TestSummary:
    def test_returns_summary(self, client):
        response = client.get("/summary?days=7")
        assert response.status_code == 200
        data = response.json()
        assert data["total_generation_mwh"] > 0
        assert data["renewable_share_percent"] > 0
        assert data["conventional_share_percent"] > 0
        assert len(data["top_producers"]) <= 5

    def test_shares_sum_to_100(self, client):
        response = client.get("/summary?days=7")
        data = response.json()
        total = data["renewable_share_percent"] + data["conventional_share_percent"]
        assert abs(total - 100.0) < 0.5


class TestFilters:
    def test_returns_available_filters(self, client):
        response = client.get("/filters")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 3
        names = {f["filter_name"] for f in data}
        assert "Photovoltaik" in names
        assert "Erdgas" in names
