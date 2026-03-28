"""
ETL Pipeline: SMARD API → SQLite.

Holt Stromerzeugungsdaten für alle Energieträger und speichert sie
in einer lokalen SQLite-Datenbank. Idempotent — kann wiederholt
ausgeführt werden, ohne Duplikate zu erzeugen.

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

from smard_client import (
    EnergyFilter,
    GENERATION_FILTERS,
    SmardClient,
    SmardTimeSeries,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path("energy.db")
API_DELAY_SECONDS = 0.5  # Höflich zur API sein


def init_db(db_path: Path) -> sqlite3.Connection:
    """Datenbank und Tabellen anlegen (falls nicht vorhanden)."""
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

        CREATE INDEX IF NOT EXISTS idx_generation_time
            ON energy_generation(timestamp_ms);

        CREATE INDEX IF NOT EXISTS idx_generation_filter
            ON energy_generation(filter_id);

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


def store_timeseries(
    conn: sqlite3.Connection,
    energy_filter: EnergyFilter,
    series: SmardTimeSeries,
) -> tuple[int, int]:
    """Zeitreihe in die DB schreiben. Gibt (inserted, skipped) zurück."""
    inserted = 0
    skipped = 0

    for point in series.data:
        if point.value_mwh is None:
            continue

        try:
            conn.execute(
                """
                INSERT INTO energy_generation
                    (timestamp_ms, timestamp_utc, filter_id, filter_name,
                     is_renewable, value_mwh)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    point.timestamp_ms,
                    point.timestamp.isoformat(),
                    energy_filter.value,
                    energy_filter.label,
                    int(energy_filter.is_renewable),
                    point.value_mwh,
                ),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1  # Duplikat — schon vorhanden

    conn.commit()
    return inserted, skipped


def run_etl(db_path: Path = DEFAULT_DB_PATH, weeks: int = 1) -> None:
    """Hauptprozess: Daten holen und speichern.

    Args:
        db_path: Pfad zur SQLite-Datei
        weeks: Anzahl der letzten Wochen, die geladen werden sollen
    """
    logger.info("=== ETL Start ===")
    logger.info(f"Datenbank: {db_path.resolve()}")
    logger.info(f"Wochen: {weeks}")
    logger.info(f"Energieträger: {len(GENERATION_FILTERS)}")

    conn = init_db(db_path)
    total_inserted = 0
    total_skipped = 0

    with SmardClient() as client:
        for energy_filter in GENERATION_FILTERS:
            logger.info(f"Lade {energy_filter.label}...")

            try:
                timestamps = client.get_timestamps(energy_filter)

                if not timestamps:
                    logger.warning(f"  Keine Timestamps für {energy_filter.label}")
                    continue

                # Die letzten N Wochen-Blöcke holen
                relevant_timestamps = timestamps[-weeks:]

                for ts in relevant_timestamps:
                    series = client.get_timeseries(energy_filter, ts)
                    inserted, skipped = store_timeseries(conn, energy_filter, series)
                    total_inserted += inserted
                    total_skipped += skipped
                    logger.info(f"  Block {ts}: {inserted} neu, {skipped} übersprungen")
                    time.sleep(API_DELAY_SECONDS)

            except Exception as e:
                logger.error(f"  Fehler bei {energy_filter.label}: {e}")
                continue

    # ETL-Lauf protokollieren
    conn.execute(
        "INSERT INTO etl_runs (filters_loaded, rows_inserted, rows_skipped) VALUES (?, ?, ?)",
        (len(GENERATION_FILTERS), total_inserted, total_skipped),
    )
    conn.commit()

    logger.info("=== ETL Fertig ===")
    logger.info(f"Gesamt: {total_inserted} eingefügt, {total_skipped} übersprungen")

    # Kurze Zusammenfassung ausgeben
    summary = pd.read_sql_query(
        """
        SELECT filter_name, is_renewable,
               COUNT(*) as datenpunkte,
               ROUND(MIN(value_mwh)) as min_mwh,
               ROUND(MAX(value_mwh)) as max_mwh,
               ROUND(AVG(value_mwh)) as avg_mwh
        FROM energy_generation
        GROUP BY filter_id
        ORDER BY avg_mwh DESC
        """,
        conn,
    )
    logger.info(f"\nDatenbestand:\n{summary.to_string(index=False)}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="SMARD Energiedaten → SQLite (Bundesnetzagentur | SMARD.de)"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Pfad zur SQLite-Datei (default: energy.db)",
    )
    parser.add_argument(
        "--weeks",
        type=int,
        default=1,
        help="Anzahl der letzten Wochen (default: 1)",
    )
    args = parser.parse_args()

    run_etl(db_path=args.db, weeks=args.weeks)
