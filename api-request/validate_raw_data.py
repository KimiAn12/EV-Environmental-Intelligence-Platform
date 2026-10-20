import os
import psycopg2

def connect_to_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "my_weather_db"),
        user=os.getenv("DB_USER", "my_weather_user"),
        password=os.getenv("DB_PASSWORD", "my_weather_password"),
    )


CHECKS = [
    {
        "name": "weather_payload_has_location",
        "layer": "raw",
        "table": "weather_api_responses",
        "sql": "select count(*) from raw.weather_api_responses where payload->'location' is null",
        "details": "Every weather payload must include a location object.",
    },
    {
        "name": "weather_temperature_in_reasonable_range",
        "layer": "raw",
        "table": "weather_api_responses",
        "sql": """
            select count(*)
            from raw.weather_api_responses
            where nullif(payload->'current'->>'temperature', '')::numeric not between -60 and 60
        """,
        "details": "Weather temperatures should be in Celsius and within an operationally plausible range.",
    },
    {
        "name": "ev_efficiency_positive",
        "layer": "raw",
        "table": "ev_fleet_records",
        "sql": "select count(*) from raw.ev_fleet_records where observed_efficiency_kwh_per_100km <= 0",
        "details": "Observed EV efficiency must be positive.",
    },
    {
        "name": "ev_city_present",
        "layer": "raw",
        "table": "ev_fleet_records",
        "sql": "select count(*) from raw.ev_fleet_records where city is null or trim(city) = ''",
        "details": "Every EV record must have a city so it can join to weather data.",
    },
]


def run_validation():
    conn = connect_to_db()
    try:
        cursor = conn.cursor()
        cursor.execute("CREATE SCHEMA IF NOT EXISTS validation")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS validation.data_quality_results (
                id BIGSERIAL PRIMARY KEY,
                check_name TEXT NOT NULL,
                layer_name TEXT NOT NULL,
                table_name TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pass', 'fail')),
                failed_count INTEGER NOT NULL DEFAULT 0,
                details TEXT,
                checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        failed_checks = []
        for check in CHECKS:
            cursor.execute(check["sql"])
            failed_count = cursor.fetchone()[0]
            status = "pass" if failed_count == 0 else "fail"
            cursor.execute("""
                INSERT INTO validation.data_quality_results (
                    check_name,
                    layer_name,
                    table_name,
                    status,
                    failed_count,
                    details
                ) VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                check["name"],
                check["layer"],
                check["table"],
                status,
                failed_count,
                check["details"],
            ))
            if status == "fail":
                failed_checks.append(f"{check['name']} failed_count={failed_count}")

        conn.commit()
        if failed_checks:
            raise ValueError("; ".join(failed_checks))
        print("Raw data validation passed.")
    finally:
        conn.close()


if __name__ == "__main__":
    run_validation()
