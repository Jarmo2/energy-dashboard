"""
Tests für smard_client.py und etl.py.

Ausführen: pytest tests/ -v
"""

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from smard_client import EnergyFilter, GENERATION_FILTERS, SmardClient, SmardDataPoint, SmardTimeSeries
from etl import init_db, store_timeseries


class TestEnergyFilter:
    def test_label_returns_readable_name(self):
        assert EnergyFilter.PHOTOVOLTAIK.label == "Photovoltaik"
        assert EnergyFilter.WIND_ONSHORE.label == "Wind Onshore"

    def test_is_renewable(self):
        assert EnergyFilter.PHOTOVOLTAIK.is_renewable is True
        assert EnergyFilter.WIND_OFFSHORE.is_renewable is True
        assert EnergyFilter.ERDGAS.is_renewable is False
        assert EnergyFilter.BRAUNKOHLE.is_renewable is False

    def test_is_generation(self):
        assert EnergyFilter.PHOTOVOLTAIK.is_generation is True
        assert EnergyFilter.VERBRAUCH_GESAMT.is_generation is False

    def test_generation_filters_excludes_consumption(self):
        for f in GENERATION_FILTERS:
            assert f.is_generation is True
        assert EnergyFilter.VERBRAUCH_GESAMT not in GENERATION_FILTERS


class TestSmardDataPoint:
    def test_timestamp_conversion(self):
        point = SmardDataPoint(timestamp_ms=1704067200000, value_mwh=5000.0)
        assert point.timestamp.year == 2024
        assert point.timestamp.month == 1

    def test_null_value(self):
        point = SmardDataPoint(timestamp_ms=1704067200000, value_mwh=None)
        assert point.value_mwh is None


class TestSmardClient:
    @patch("smard_client.requests.Session")
    def test_get_timestamps(self, mock_session_cls):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"timestamps": [1704067200000, 1704672000000]}
        mock_resp.raise_for_status = MagicMock()
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        client = SmardClient()
        timestamps = client.get_timestamps(EnergyFilter.PHOTOVOLTAIK)
        assert len(timestamps) == 2
        assert timestamps == sorted(timestamps)

    @patch("smard_client.requests.Session")
    def test_get_timeseries(self, mock_session_cls):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"series": [[1704067200000, 1200.5], [1704070800000, None]]}
        mock_resp.raise_for_status = MagicMock()
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        client = SmardClient()
        ts = client.get_timeseries(EnergyFilter.PHOTOVOLTAIK, 1704067200000)
        assert len(ts.data) == 2
        assert ts.data[0].value_mwh == 1200.5
        assert ts.data[1].value_mwh is None

    @patch("smard_client.requests.Session")
    def test_url_construction(self, mock_session_cls):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"series": []}
        mock_resp.raise_for_status = MagicMock()
        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        client = SmardClient()
        client.get_timeseries(EnergyFilter.ERDGAS, 1704067200000)
        called_url = mock_session.get.call_args[0][0]
        assert "/4071/DE/4071_DE_hour_1704067200000.json" in called_url


class TestDatabase:
    @pytest.fixture
    def db(self, tmp_path):
        conn = init_db(tmp_path / "test.db")
        yield conn
        conn.close()

    def test_tables_created(self, db):
        tables = db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        table_names = {t[0] for t in tables}
        assert "energy_generation" in table_names
        assert "etl_runs" in table_names

    def test_store_and_retrieve(self, db):
        series = SmardTimeSeries(filter_id=4068, region="DE", resolution="hour", data=[
            SmardDataPoint(timestamp_ms=1704067200000, value_mwh=5000.0),
            SmardDataPoint(timestamp_ms=1704070800000, value_mwh=5500.0),
        ])
        inserted, skipped = store_timeseries(db, EnergyFilter.PHOTOVOLTAIK, series)
        assert inserted == 2
        assert skipped == 0

    def test_idempotent_insert(self, db):
        series = SmardTimeSeries(filter_id=4068, region="DE", resolution="hour",
                                data=[SmardDataPoint(timestamp_ms=1704067200000, value_mwh=5000.0)])
        store_timeseries(db, EnergyFilter.PHOTOVOLTAIK, series)
        inserted, skipped = store_timeseries(db, EnergyFilter.PHOTOVOLTAIK, series)
        assert inserted == 0
        assert skipped == 1

    def test_null_values_skipped(self, db):
        series = SmardTimeSeries(filter_id=4068, region="DE", resolution="hour",
                                data=[SmardDataPoint(timestamp_ms=1704067200000, value_mwh=None)])
        inserted, _ = store_timeseries(db, EnergyFilter.PHOTOVOLTAIK, series)
        assert inserted == 0
