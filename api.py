"""
Energy Dashboard API — Strommarktdaten als REST-Endpunkte.

Starten:
    uvicorn api:app --reload --port 8000

Dokumentation:
    http://localhost:8000/docs  (Swagger UI)
    http://localhost:8000/redoc (ReDoc)

Datenquelle: Bundesnetzagentur | SMARD.de (CC BY 4.0)
"""

import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

DB_PATH = Path("energy.db")


# --- Pydantic Models für die API-Antworten ---


class EnergyMixEntry(BaseModel):
    """Ein Energieträger im aktuellen Mix."""

    filter_name: str
    is_renewable: bool
    total_mwh: float
    share_percent: float


class EnergyMixResponse(BaseModel):
    """Antwort für /energy-mix."""

    period_start: str
    period_end: str
    total_generation_mwh: float
    renewable_share_percent: float
    mix: list[EnergyMixEntry]


class TimeSeriesPoint(BaseModel):
    """Ein Datenpunkt in der Zeitreihe."""

    timestamp_utc: str
    value_mwh: Optional[float]


class TimeSeriesResponse(BaseModel):
    """Antwort für /timeseries."""

    filter_name: str
    resolution: str
    data_points: int
    data: list[TimeSeriesPoint]


class SummaryResponse(BaseModel):
    """Antwort für /summary."""

    period_start: str
    period_end: str
    total_generation_mwh: float
    renewable_share_percent: float
    conventional_share_percent: float
    top_producers: list[dict]
    data_points_total: int


# --- Datenbank-Verbindung ---


def get_db() -> sqlite3.Connection:
    """SQLite-Verbindung mit Row-Factory für dict-artigen Zugriff."""
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=503,
            detail="Datenbank nicht gefunden. Bitte zuerst 'python etl.py' ausführen.",
        )
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# --- App ---


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Prüft beim Start, ob die Datenbank existiert."""
    if not DB_PATH.exists():
        print(f"WARNUNG: {DB_PATH} nicht gefunden. Bitte 'python etl.py' ausführen.")
    yield


app = FastAPI(
    title="Hannover Energy Dashboard API",
    description="Strommarktdaten der Bundesnetzagentur (SMARD.de), "
    "aufbereitet über eine Python-ETL-Pipeline.",
    version="0.1.0",
    lifespan=lifespan,
)


# --- Endpunkte ---


@app.get("/energy-mix", response_model=EnergyMixResponse)
def get_energy_mix(
    days: int = Query(default=7, ge=1, le=365, description="Anzahl der letzten Tage"),
):
    """Energiemix der letzten N Tage.

    Gibt die prozentuale Verteilung der Stromerzeugung nach Energieträger zurück.
    """
    conn = get_db()
    try:
        cutoff_ms = int(
            (datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000
        )

        rows = conn.execute(
            """
            SELECT filter_name, is_renewable,
                   ROUND(SUM(value_mwh), 1) as total_mwh
            FROM energy_generation
            WHERE timestamp_ms >= ?
            GROUP BY filter_id
            ORDER BY total_mwh DESC
            """,
            (cutoff_ms,),
        ).fetchall()

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"Keine Daten für die letzten {days} Tage gefunden.",
            )

        grand_total = sum(row["total_mwh"] for row in rows)

        mix = [
            EnergyMixEntry(
                filter_name=row["filter_name"],
                is_renewable=bool(row["is_renewable"]),
                total_mwh=row["total_mwh"],
                share_percent=round(row["total_mwh"] / grand_total * 100, 1)
                if grand_total > 0
                else 0,
            )
            for row in rows
        ]

        # Zeitraum bestimmen
        time_range = conn.execute(
            "SELECT MIN(timestamp_utc) as start, MAX(timestamp_utc) as end "
            "FROM energy_generation WHERE timestamp_ms >= ?",
            (cutoff_ms,),
        ).fetchone()

        renewable_mwh = sum(e.total_mwh for e in mix if e.is_renewable)

        return EnergyMixResponse(
            period_start=time_range["start"],
            period_end=time_range["end"],
            total_generation_mwh=round(grand_total, 1),
            renewable_share_percent=round(renewable_mwh / grand_total * 100, 1)
            if grand_total > 0
            else 0,
            mix=mix,
        )
    finally:
        conn.close()


@app.get("/timeseries", response_model=TimeSeriesResponse)
def get_timeseries(
    filter_name: str = Query(
        description="Name des Energieträgers, z.B. 'Photovoltaik', 'Wind Onshore', 'Erdgas'"
    ),
    days: int = Query(default=7, ge=1, le=365, description="Anzahl der letzten Tage"),
):
    """Zeitreihe für einen bestimmten Energieträger.

    Gibt stündliche MWh-Werte für die letzten N Tage zurück.
    """
    conn = get_db()
    try:
        cutoff_ms = int(
            (datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000
        )

        # Prüfen ob der Filter existiert
        available = conn.execute(
            "SELECT DISTINCT filter_name FROM energy_generation"
        ).fetchall()
        available_names = [row["filter_name"] for row in available]

        if filter_name not in available_names:
            raise HTTPException(
                status_code=404,
                detail=f"Energieträger '{filter_name}' nicht gefunden. "
                f"Verfügbar: {', '.join(available_names)}",
            )

        rows = conn.execute(
            """
            SELECT timestamp_utc, value_mwh
            FROM energy_generation
            WHERE filter_name = ? AND timestamp_ms >= ?
            ORDER BY timestamp_ms
            """,
            (filter_name, cutoff_ms),
        ).fetchall()

        data = [
            TimeSeriesPoint(
                timestamp_utc=row["timestamp_utc"], value_mwh=row["value_mwh"]
            )
            for row in rows
        ]

        return TimeSeriesResponse(
            filter_name=filter_name,
            resolution="hour",
            data_points=len(data),
            data=data,
        )
    finally:
        conn.close()


@app.get("/summary", response_model=SummaryResponse)
def get_summary(
    days: int = Query(default=7, ge=1, le=365, description="Anzahl der letzten Tage"),
):
    """Zusammenfassung des Energiemix.

    Gibt Gesamterzeugung, Anteil Erneuerbarer und Top-5-Erzeuger zurück.
    """
    conn = get_db()
    try:
        cutoff_ms = int(
            (datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000
        )

        # Top-Erzeuger
        rows = conn.execute(
            """
            SELECT filter_name, is_renewable,
                   ROUND(SUM(value_mwh), 1) as total_mwh
            FROM energy_generation
            WHERE timestamp_ms >= ?
            GROUP BY filter_id
            ORDER BY total_mwh DESC
            LIMIT 5
            """,
            (cutoff_ms,),
        ).fetchall()

        if not rows:
            raise HTTPException(
                status_code=404,
                detail=f"Keine Daten für die letzten {days} Tage.",
            )

        # Gesamtwerte
        totals = conn.execute(
            """
            SELECT
                ROUND(SUM(value_mwh), 1) as total,
                ROUND(SUM(CASE WHEN is_renewable = 1 THEN value_mwh ELSE 0 END), 1) as renewable,
                COUNT(*) as data_points,
                MIN(timestamp_utc) as period_start,
                MAX(timestamp_utc) as period_end
            FROM energy_generation
            WHERE timestamp_ms >= ?
            """,
            (cutoff_ms,),
        ).fetchone()

        total = totals["total"] or 0
        renewable = totals["renewable"] or 0

        return SummaryResponse(
            period_start=totals["period_start"],
            period_end=totals["period_end"],
            total_generation_mwh=total,
            renewable_share_percent=round(renewable / total * 100, 1)
            if total > 0
            else 0,
            conventional_share_percent=round((total - renewable) / total * 100, 1)
            if total > 0
            else 0,
            top_producers=[
                {
                    "name": row["filter_name"],
                    "is_renewable": bool(row["is_renewable"]),
                    "total_mwh": row["total_mwh"],
                }
                for row in rows
            ],
            data_points_total=totals["data_points"],
        )
    finally:
        conn.close()


@app.get("/filters")
def get_available_filters():
    """Liste aller verfügbaren Energieträger in der Datenbank."""
    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT filter_name, is_renewable, filter_id
            FROM energy_generation
            ORDER BY filter_name
            """
        ).fetchall()

        return [
            {
                "filter_name": row["filter_name"],
                "filter_id": row["filter_id"],
                "is_renewable": bool(row["is_renewable"]),
            }
            for row in rows
        ]
    finally:
        conn.close()
