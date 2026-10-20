import os
import json
import hashlib
from pathlib import Path

import psycopg2
from psycopg2.extras import Json

from api_request import configured_locations, get_weather_payload

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EV_DATA_PATH = Path(os.getenv("EV_DATA_PATH", PROJECT_ROOT / "data" / "ev" / "fleet_operations_sample.json"))


def connect_to_db():
    print("connecting to db")
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            dbname=os.getenv("DB_NAME", "my_weather_db"),
            user=os.getenv("DB_USER", "my_weather_user"),
            password=os.getenv("DB_PASSWORD", "my_weather_password"),
        )
    except psycopg2.Error as e:
        print(f"Database connection failed: {e}")
        raise


def create_table(conn):
    print("creating warehouse schemas and raw tables if not exists")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE SCHEMA IF NOT EXISTS validation;
            CREATE SCHEMA IF NOT EXISTS analytics;

            CREATE TABLE IF NOT EXISTS raw.weather_api_responses (
                id BIGSERIAL PRIMARY KEY,
                source TEXT NOT NULL DEFAULT 'weatherstack',
                location_query TEXT NOT NULL,
                payload JSONB NOT NULL,
                fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                payload_hash TEXT NOT NULL UNIQUE
            );

            CREATE TABLE IF NOT EXISTS raw.ev_fleet_records (
                vehicle_id TEXT PRIMARY KEY,
                model TEXT NOT NULL,
                region TEXT NOT NULL,
                city TEXT NOT NULL,
                battery_kwh NUMERIC(8,2) NOT NULL,
                rated_efficiency_kwh_per_100km NUMERIC(8,2) NOT NULL,
                observed_efficiency_kwh_per_100km NUMERIC(8,2) NOT NULL,
                odometer_km NUMERIC(12,2) NOT NULL,
                operating_date DATE NOT NULL,
                payload JSONB NOT NULL,
                loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            CREATE TABLE IF NOT EXISTS validation.data_quality_results (
                id BIGSERIAL PRIMARY KEY,
                check_name TEXT NOT NULL,
                layer_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pass', 'fail')),
                failed_count INTEGER NOT NULL DEFAULT 0,
                details TEXT,
                checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
        conn.commit()
        print("Warehouse schemas and tables are ready.")
    except psycopg2.Error as e:
        print(f"Failed to create table: {e}")
        raise


def _payload_hash(payload):
    encoded = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def insert_weather_payload(conn, location_query, payload):
    print(f"inserting weather payload for {location_query}")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO raw.weather_api_responses (
                source,
                location_query,
                payload,
                payload_hash
            ) VALUES (%s, %s, %s, %s)
            ON CONFLICT (payload_hash) DO NOTHING
        """, (
            "weatherstack",
            location_query,
            Json(payload),
            _payload_hash(payload),
        ))
        conn.commit()
        print("Weather payload inserted or already present.")
    except psycopg2.Error as e:
        print(f"Failed to insert weather payload: {e}")
        raise


def ingest_weather():
    try:
        conn = connect_to_db()
        create_table(conn)
        for location in configured_locations():
            insert_weather_payload(conn, location, get_weather_payload(location))
    except Exception as e:
        print(f"An error occurred during weather ingestion: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")


def ingest_ev_data():
    try:
        conn = connect_to_db()
        create_table(conn)
        records = json.loads(EV_DATA_PATH.read_text())
        cursor = conn.cursor()
        for record in records:
            cursor.execute("""
                INSERT INTO raw.ev_fleet_records (
                    vehicle_id,
                    model,
                    region,
                    city,
                    battery_kwh,
                    rated_efficiency_kwh_per_100km,
                    observed_efficiency_kwh_per_100km,
                    odometer_km,
                    operating_date,
                    payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (vehicle_id) DO UPDATE SET
                    model = EXCLUDED.model,
                    region = EXCLUDED.region,
                    city = EXCLUDED.city,
                    battery_kwh = EXCLUDED.battery_kwh,
                    rated_efficiency_kwh_per_100km = EXCLUDED.rated_efficiency_kwh_per_100km,
                    observed_efficiency_kwh_per_100km = EXCLUDED.observed_efficiency_kwh_per_100km,
                    odometer_km = EXCLUDED.odometer_km,
                    operating_date = EXCLUDED.operating_date,
                    payload = EXCLUDED.payload,
                    loaded_at = NOW()
            """, (
                record["vehicle_id"],
                record["model"],
                record["region"],
                record["city"],
                record["battery_kwh"],
                record["rated_efficiency_kwh_per_100km"],
                record["observed_efficiency_kwh_per_100km"],
                record["odometer_km"],
                record["operating_date"],
                Json(record),
            ))
        conn.commit()
        print(f"Loaded {len(records)} EV fleet records.")
    except Exception as e:
        print(f"An error occurred during EV ingestion: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")


def main():
    ingest_weather()
    ingest_ev_data()


if __name__ == "__main__":
    main()
