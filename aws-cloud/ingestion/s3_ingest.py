"""Validate local EV/weather inputs and write raw JSON objects to S3."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import os
import re
import sys
import unicodedata
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EV_DATA_PATH = PROJECT_ROOT / "data" / "ev" / "fleet_operations_sample.json"
WEATHERSTACK_URL = "https://api.weatherstack.com/current"
EV_REQUIRED_FIELDS = {
    "vehicle_id",
    "model",
    "region",
    "city",
    "battery_kwh",
    "rated_efficiency_kwh_per_100km",
    "observed_efficiency_kwh_per_100km",
    "odometer_km",
    "operating_date",
}

logger = logging.getLogger("aws_cloud.ingestion")


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _ingest_date(value: str | date | None) -> str:
    if value is None:
        return datetime.now(timezone.utc).date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise ValueError(f"Invalid ingest date {value!r}; expected YYYY-MM-DD") from exc


def _slug(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", normalized).strip("-").lower()
    return slug or "unknown"


def validate_ev_records(records: Any) -> list[dict[str, Any]]:
    if not isinstance(records, list) or not records:
        raise ValueError("EV input must be a non-empty JSON array of records")

    validated: list[dict[str, Any]] = []
    seen_vehicle_ids: set[str] = set()
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise ValueError(f"EV record at index {index} must be a JSON object")
        missing = EV_REQUIRED_FIELDS - record.keys()
        if missing:
            raise ValueError(f"EV record at index {index} is missing fields: {', '.join(sorted(missing))}")
        for field in ("vehicle_id", "model", "region", "city"):
            if not isinstance(record[field], str) or not record[field].strip():
                raise ValueError(f"EV record at index {index} has an empty or invalid {field}")
        vehicle_id = record["vehicle_id"]
        if vehicle_id in seen_vehicle_ids:
            raise ValueError(f"Duplicate vehicle_id {vehicle_id!r} in EV input")
        seen_vehicle_ids.add(vehicle_id)
        try:
            operating_date = date.fromisoformat(record["operating_date"])
            battery = float(record["battery_kwh"])
            rated_efficiency = float(record["rated_efficiency_kwh_per_100km"])
            observed_efficiency = float(record["observed_efficiency_kwh_per_100km"])
            odometer = float(record["odometer_km"])
        except (TypeError, ValueError) as exc:
            raise ValueError(f"EV record at index {index} has an invalid date or numeric value") from exc
        measurements = (battery, rated_efficiency, observed_efficiency, odometer)
        if not all(math.isfinite(value) for value in measurements):
            raise ValueError(f"EV record at index {index} has a non-finite measurement")
        if battery <= 0 or rated_efficiency <= 0 or observed_efficiency <= 0 or odometer < 0:
            raise ValueError(f"EV record at index {index} has invalid non-positive measurements")
        if operating_date.isoformat() != record["operating_date"]:
            raise ValueError(f"EV record at index {index} operating_date must use YYYY-MM-DD")
        validated.append(record)
    return validated


def validate_weather_payload(payload: Any) -> None:
    if not isinstance(payload, dict) or not isinstance(payload.get("location"), dict):
        raise ValueError("Weather payload must contain a location object")
    if not isinstance(payload.get("current"), dict):
        raise ValueError("Weather payload must contain a current object")
    if not payload["location"].get("name"):
        raise ValueError("Weather payload location.name is required")


def create_s3_client():
    """Use the AWS SDK default credential chain or the named AWS_PROFILE."""
    try:
        import boto3
    except ImportError as exc:
        raise RuntimeError("boto3 is required; install dependencies with `python -m pip install -r requirements.txt`") from exc

    profile = os.getenv("AWS_PROFILE") or None
    region = os.getenv("AWS_REGION") or None
    session = boto3.Session(profile_name=profile, region_name=region)
    return session.client("s3")


def upload_ev_file(
    s3_client,
    bucket: str,
    source_path: str | Path = DEFAULT_EV_DATA_PATH,
    ingest_date: str | date | None = None,
) -> str:
    source = Path(source_path)
    try:
        records = validate_ev_records(json.loads(source.read_text(encoding="utf-8")))
    except OSError as exc:
        raise RuntimeError(f"Could not read EV input file: {source}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"EV input is not valid JSON: {source}") from exc

    partition_date = _ingest_date(ingest_date)
    # Add the ingestion partition to each row so Spark can use it after reading JSON Lines.
    rows = [{**record, "ingest_date": partition_date} for record in records]
    body = b"\n".join(_canonical_json(row) for row in rows) + b"\n"
    digest = _sha256(body)
    key = f"raw/ev/ingest_date={partition_date}/{source.stem}-{digest[:16]}.jsonl"
    _put_object(s3_client, bucket, key, body, "application/x-ndjson")
    logger.info("Uploaded %d EV records to s3://%s/%s", len(records), bucket, key)
    return key


def upload_weather_payload(
    s3_client,
    bucket: str,
    location_query: str,
    payload: dict[str, Any],
    ingest_date: str | date | None = None,
    source: str = "weatherstack",
) -> str:
    validate_weather_payload(payload)
    partition_date = _ingest_date(ingest_date)
    envelope = {
        "source": source,
        "location_query": location_query,
        "ingest_date": partition_date,
        "payload_hash": _sha256(_canonical_json(payload)),
        "payload": payload,
    }
    body = _canonical_json(envelope) + b"\n"
    digest = _sha256(body)
    city = str(payload["location"]["name"])
    key = (
        f"raw/weather/ingest_date={partition_date}/location={_slug(city)}/"
        f"weather-{digest[:16]}.json"
    )
    _put_object(s3_client, bucket, key, body, "application/json")
    logger.info("Uploaded weather payload for %s to s3://%s/%s", location_query, bucket, key)
    return key


def _put_object(s3_client, bucket: str, key: str, body: bytes, content_type: str) -> None:
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)
    except Exception:
        logger.exception("S3 upload failed for s3://%s/%s", bucket, key)
        raise


def configured_locations() -> list[str]:
    raw_locations = os.getenv("WEATHER_LOCATIONS", "New York,Detroit,Austin,Fremont,Toronto")
    locations = [location.strip() for location in raw_locations.split(",") if location.strip()]
    if not locations:
        raise ValueError("WEATHER_LOCATIONS must contain at least one location")
    return locations


def mock_weather_payload(location: str) -> dict[str, Any]:
    """Return the same Weatherstack-shaped sample used by the local ingestion project."""
    city_profiles = {
        "New York": ("United States of America", "New York", 5, "Mist", 10, "-5.0"),
        "Detroit": ("United States of America", "Michigan", -2, "Light snow", 18, "-5.0"),
        "Austin": ("United States of America", "Texas", 24, "Sunny", 12, "-6.0"),
        "Fremont": ("United States of America", "California", 17, "Partly cloudy", 9, "-8.0"),
        "Toronto": ("Canada", "Ontario", -6, "Overcast", 16, "-5.0"),
    }
    country, region, temperature, description, wind_speed, utc_offset = city_profiles.get(
        location, city_profiles["New York"]
    )
    localtime = datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    return {
        "request": {"type": "City", "query": f"{location}, {country}", "language": "en", "unit": "m"},
        "location": {
            "name": location,
            "country": country,
            "region": region,
            "lat": "0.000",
            "lon": "0.000",
            "timezone_id": "UTC",
            "localtime": localtime,
            "utc_offset": utc_offset,
        },
        "current": {
            "observation_time": "12:00 AM",
            "temperature": temperature,
            "weather_code": 116,
            "weather_descriptions": [description],
            "wind_speed": wind_speed,
            "pressure": 1012,
            "precip": 0,
            "humidity": 72,
            "cloudcover": 40,
            "feelslike": temperature - 2,
            "uv_index": 2,
            "visibility": 10,
            "is_day": "yes",
        },
    }


def fetch_weather_payload(location: str, use_mock: bool | None = None) -> tuple[dict[str, Any], str]:
    if use_mock is None:
        use_mock = os.getenv("USE_MOCK_WEATHER", "true").strip().lower() in {"1", "true", "yes"}
    if use_mock:
        return mock_weather_payload(location), "mock"

    api_key = os.getenv("WEATHERSTACK_API_KEY", "")
    if not api_key:
        raise ValueError("WEATHERSTACK_API_KEY is required when USE_MOCK_WEATHER=false")
    try:
        import requests
    except ImportError as exc:
        raise RuntimeError("requests is required for live weather; install requirements.txt") from exc
    try:
        response = requests.get(WEATHERSTACK_URL, params={"access_key": api_key, "query": location}, timeout=30)
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        # Requests exceptions can include the full URL, including the API key query parameter.
        raise RuntimeError(f"Weatherstack request failed for location {location!r}") from exc
    if payload.get("success") is False:
        # Do not include the full provider response in logs; it may echo request details.
        raise ValueError(f"Weatherstack returned an error for location {location!r}")
    validate_weather_payload(payload)
    return payload, "weatherstack"


def ingest_ev(s3_client, bucket: str, source_path: str | Path, ingest_date: str | date | None = None) -> str:
    return upload_ev_file(s3_client, bucket, source_path, ingest_date)


def ingest_weather(
    s3_client,
    bucket: str,
    locations: list[str] | None = None,
    ingest_date: str | date | None = None,
    payload_provider: Callable[[str], tuple[dict[str, Any], str]] = fetch_weather_payload,
) -> list[str]:
    keys = []
    for location in locations or configured_locations():
        payload, source = payload_provider(location)
        keys.append(upload_weather_payload(s3_client, bucket, location, payload, ingest_date, source))
    return keys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Upload EV and weather raw JSON data to the S3 data lake.")
    parser.add_argument("dataset", choices=("ev", "weather", "all"), help="Dataset(s) to ingest")
    parser.add_argument("--ev-file", default=os.getenv("EV_DATA_PATH", str(DEFAULT_EV_DATA_PATH)))
    parser.add_argument("--ingest-date", help="Partition date in YYYY-MM-DD (defaults to current UTC date)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO").upper(), format="%(asctime)s %(levelname)s %(message)s")
    bucket = os.getenv("S3_BUCKET_NAME", "").strip()
    if not bucket or bucket == "replace-with-globally-unique-name":
        parser.error("Set S3_BUCKET_NAME to the deployed bucket name")

    try:
        client = create_s3_client()
        if args.dataset in {"ev", "all"}:
            ingest_ev(client, bucket, args.ev_file, args.ingest_date)
        if args.dataset in {"weather", "all"}:
            ingest_weather(client, bucket, ingest_date=args.ingest_date)
    except Exception as exc:
        logger.error("Ingestion failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
