"""
ETL Pipeline: SMARD API → SQLite.

Nutzung:
    python etl.py                     # Neueste Woche laden
    python etl.py --weeks 4           # Letzte 4 Wochen laden
    python etl.py --db energy.db      # Andere DB-Datei
"""

import argparse
import logging
import sqlite3
import time
from pathlib import Path

import pandas as pd

from smard_client import EnergyFilter, GENERATION_FILTERS, SmardClient, SmardTimeSeries

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path("energy.db")
API_DELAY_SECONDS = 0.5


def init_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS energy_generation (
            timestamp_ms    INTEGER NOT NULL,
            timestamp_utc   TEXT NOT NULL,
            filter_id       INTEGER NOT NULL,
            filter_name     TEXT NOT NULL,
            is_renewable    INTEGER NOT NULL,
            value_mwh       REAL,
            PRIMARY KEY (timestamp_ms, filter_id)
        );
        CREATE INDEX IF NOT EXISTS idx_generation_time ON energy_generation(timestamp_ms);
        CREATE INDEX IF NOT EXISTS idx_generation_filter ON energy_generation(filter_id);
        CREATE TABLE IF NOT EXISTS etl_runs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            run_at          TEXT NOT NULL DEFAULT (datetime('now')),
            filters_loaded  INTEGER NOT NULL,
            rows_inserted   INTEGER NOT NULL,
            rows_skipped    INTEGER NOT NULL
        );
    """)
    conn.commit()
    return conn


def store_timeseries(conn, energy_filter, series):
    inserted = skipped = 0
    for point in series.data:
        if point.value_mwh is None:
            continue
        try:
            conn.execute(
                "INSERT INTO energy_generation (timestamp_ms, timestamp_utc, filter_id, "
                "filter_name, is_renewable, value_mwh) VALUES (?, ?, ?, ?, ?, ?)",
                (point.timestamp_ms, point.timestamp.isoformat(), energy_filter.value,
                 energy_filter.label, int(energy_filter.is_renewable), point.value_mwh))
            inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1
    conn.commit()
    return inserted, skipped


def run_etl(db_path=DEFAULT_DB_PATH, weeks=1):
    logger.info("=== ETL Start ===")
    logger.info(f"Datenbank: {db_path.resolve()}, Wochen: {weeks}")
    conn = init_db(db_path)
    total_inserted = total_skipped = 0

    with SmardClient() as client:
        for energy_filter in GENERATION_FILTERS:
            logger.info(f"Lade {energy_filter.label}...")
            try:
                timestamps = client.get_timestamps(energy_filter)
                if not timestamps:
                    logger.warning(f"  Keine Timestamps für {energy_filter.label}")
                    continue
                for ts in timestamps[-weeks:]:
                    series = client.get_timeseries(energy_filter, ts)
                    inserted, skipped = store_timeseries(conn, energy_filter, series)
                    total_inserted += inserted
                    total_skipped += skipped
                    logger.info(f"  Block {ts}: {inserted} neu, {skipped} übersprungen")
                    time.sleep(API_DELAY_SECONDS)
            except Exception as e:
                logger.error(f"  Fehler bei {energy_filter.label}: {e}")

    conn.execute("INSERT INTO etl_runs (filters_loaded, rows_inserted, rows_skipped) VALUES (?, ?, ?)",
                 (len(GENERATION_FILTERS), total_inserted, total_skipped))
    conn.commit()
    logger.info(f"=== ETL Fertig === {total_inserted} eingefügt, {total_skipped} übersprungen")

    summary = pd.read_sql_query(
        "SELECT filter_name, is_renewable, COUNT(*) as datenpunkte, "
        "ROUND(MIN(value_mwh)) as min_mwh, ROUND(MAX(value_mwh)) as max_mwh, "
        "ROUND(AVG(value_mwh)) as avg_mwh FROM energy_generation "
        "GROUP BY filter_id ORDER BY avg_mwh DESC", conn)
    logger.info(f"\nDatenbestand:\n{summary.to_string(index=False)}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SMARD Energiedaten → SQLite")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--weeks", type=int, default=1)
    args = parser.parse_args()
    run_etl(db_path=args.db, weeks=args.weeks)
