"""
SMARD API Client — Strommarktdaten der Bundesnetzagentur.

Datenquelle: https://smard.de (CC BY 4.0, Bundesnetzagentur | SMARD.de)

Die API liefert Zeitreihendaten zur Stromerzeugung, Verbrauch und Marktpreisen.
Zwei Schritte: (1) Timestamps abrufen, (2) Zeitreihendaten für einen Timestamp laden.
"""

from datetime import datetime, timezone
from enum import IntEnum
from typing import Optional

import requests
from pydantic import BaseModel, Field

BASE_URL = "https://www.smard.de/app/chart_data"
DEFAULT_REGION = "DE"
DEFAULT_RESOLUTION = "hour"
REQUEST_TIMEOUT = 30


class EnergyFilter(IntEnum):
    BRAUNKOHLE = 1223
    KERNENERGIE = 1224
    WIND_OFFSHORE = 1225
    WASSERKRAFT = 1226
    SONSTIGE_KONVENTIONELLE = 1227
    SONSTIGE_ERNEUERBARE = 1228
    BIOMASSE = 4066
    WIND_ONSHORE = 4067
    PHOTOVOLTAIK = 4068
    STEINKOHLE = 4069
    PUMPSPEICHER = 4070
    ERDGAS = 4071
    VERBRAUCH_GESAMT = 410
    RESIDUALLAST = 4359
    VERBRAUCH_PUMPSPEICHER = 4387

    @property
    def label(self) -> str:
        labels = {
            1223: "Braunkohle", 1224: "Kernenergie", 1225: "Wind Offshore",
            1226: "Wasserkraft", 1227: "Sonstige Konventionelle",
            1228: "Sonstige Erneuerbare", 4066: "Biomasse", 4067: "Wind Onshore",
            4068: "Photovoltaik", 4069: "Steinkohle", 4070: "Pumpspeicher",
            4071: "Erdgas", 410: "Verbrauch Gesamt", 4359: "Residuallast",
            4387: "Verbrauch Pumpspeicher",
        }
        return labels[self.value]

    @property
    def is_renewable(self) -> bool:
        return self in {
            EnergyFilter.WIND_OFFSHORE, EnergyFilter.WIND_ONSHORE,
            EnergyFilter.PHOTOVOLTAIK, EnergyFilter.WASSERKRAFT,
            EnergyFilter.BIOMASSE, EnergyFilter.SONSTIGE_ERNEUERBARE,
        }

    @property
    def is_generation(self) -> bool:
        return self not in {
            EnergyFilter.VERBRAUCH_GESAMT, EnergyFilter.RESIDUALLAST,
            EnergyFilter.VERBRAUCH_PUMPSPEICHER,
        }


GENERATION_FILTERS = [f for f in EnergyFilter if f.is_generation]


class SmardDataPoint(BaseModel):
    timestamp_ms: int = Field(description="Unix timestamp in Millisekunden")
    value_mwh: Optional[float] = Field(default=None, description="Wert in MWh")

    @property
    def timestamp(self) -> datetime:
        return datetime.fromtimestamp(self.timestamp_ms / 1000, tz=timezone.utc)


class SmardTimeSeries(BaseModel):
    filter_id: int
    region: str
    resolution: str
    data: list[SmardDataPoint]

    @property
    def filter_name(self) -> str:
        try:
            return EnergyFilter(self.filter_id).label
        except ValueError:
            return f"Filter {self.filter_id}"


class SmardClient:
    def __init__(self, base_url=BASE_URL, region=DEFAULT_REGION,
                 resolution=DEFAULT_RESOLUTION, timeout=REQUEST_TIMEOUT):
        self.base_url = base_url
        self.region = region
        self.resolution = resolution
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def get_timestamps(self, energy_filter: EnergyFilter) -> list[int]:
        url = f"{self.base_url}/{energy_filter.value}/{self.region}/index_{self.resolution}.json"
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        return sorted(response.json().get("timestamps", []))

    def get_timeseries(self, energy_filter: EnergyFilter, timestamp: int) -> SmardTimeSeries:
        fid = energy_filter.value
        url = f"{self.base_url}/{fid}/{self.region}/{fid}_{self.region}_{self.resolution}_{timestamp}.json"
        response = self.session.get(url, timeout=self.timeout)
        response.raise_for_status()
        raw = response.json()
        data_points = [
            SmardDataPoint(timestamp_ms=point[0], value_mwh=point[1])
            for point in raw.get("series", [])
            if isinstance(point, list) and len(point) >= 2
        ]
        return SmardTimeSeries(filter_id=fid, region=self.region,
                               resolution=self.resolution, data=data_points)

    def get_latest_timeseries(self, energy_filter: EnergyFilter) -> SmardTimeSeries:
        timestamps = self.get_timestamps(energy_filter)
        if not timestamps:
            return SmardTimeSeries(filter_id=energy_filter.value, region=self.region,
                                  resolution=self.resolution, data=[])
        return self.get_timeseries(energy_filter, timestamps[-1])

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
