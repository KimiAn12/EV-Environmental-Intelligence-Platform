import json
import os
import requests
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingestion.s3_ingest import (  # noqa: E402
    DEFAULT_EV_DATA_PATH,
    fetch_weather_payload,
    upload_ev_file,
    upload_weather_payload,
    validate_ev_records,
    validate_weather_payload,
)


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)
        object_key = (kwargs["Bucket"], kwargs["Key"])
        if kwargs.get("IfNoneMatch") == "*" and object_key in self.objects:
            raise FakePreconditionFailed()
        self.objects[object_key] = kwargs


class FakePreconditionFailed(Exception):
    response = {
        "Error": {"Code": "PreconditionFailed"},
        "ResponseMetadata": {"HTTPStatusCode": 412},
    }


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self.payload = payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)

    def json(self):
        return self.payload


class S3IngestionTests(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3()

    def test_ev_upload_is_partitioned_and_repeatable_for_same_content(self):
        records = [{
            "vehicle_id": "EV-1",
            "model": "Van",
            "region": "West",
            "city": "Fremont",
            "battery_kwh": 60,
            "rated_efficiency_kwh_per_100km": 18,
            "observed_efficiency_kwh_per_100km": 20,
            "odometer_km": 1000,
            "operating_date": "2026-10-29",
        }]
        with tempfile.TemporaryDirectory() as temp_dir:
            source = Path(temp_dir) / "fleet.json"
            source.write_text(json.dumps(records), encoding="utf-8")
            first = upload_ev_file(self.s3, "example-bucket", source, date(2026, 10, 30))
            second = upload_ev_file(self.s3, "example-bucket", source, date(2026, 10, 30))

        self.assertEqual(first, second)
        self.assertIn("raw/ev/ingest_date=2026-10-30/", first)
        self.assertEqual(len(self.s3.objects), 1)
        self.assertEqual(len(self.s3.calls), 2)
        self.assertEqual(self.s3.calls[0]["IfNoneMatch"], "*")
        uploaded = next(iter(self.s3.objects.values()))
        self.assertEqual(uploaded["ContentType"], "application/x-ndjson")
        row = json.loads(uploaded["Body"].decode().splitlines()[0])
        self.assertEqual(row["ingest_date"], "2026-10-30")
        self.assertEqual(row["source"], "ev_fleet_file")
        self.assertEqual(row["source_file"], "fleet.json")
        self.assertTrue(row["ingested_at"].endswith("Z"))

    def test_project_ev_sample_uploads_five_valid_records(self):
        key = upload_ev_file(self.s3, "example-bucket", DEFAULT_EV_DATA_PATH, "2026-10-30")
        uploaded = next(iter(self.s3.objects.values()))
        rows = [json.loads(line) for line in uploaded["Body"].decode().splitlines()]

        self.assertIn("raw/ev/ingest_date=2026-10-30/", key)
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[0]["vehicle_id"], "EV-1001")

    def test_weather_upload_preserves_payload_in_json_envelope(self):
        payload = {"location": {"name": "New York"}, "current": {"temperature": 15}}
        key = upload_weather_payload(self.s3, "example-bucket", "New York", payload, "2026-10-30", "mock")
        duplicate_key = upload_weather_payload(
            self.s3, "example-bucket", "New York", payload, "2026-10-30", "mock"
        )
        uploaded = next(iter(self.s3.objects.values()))
        envelope = json.loads(uploaded["Body"])

        self.assertIn("raw/weather/ingest_date=2026-10-30/location=new-york/", key)
        self.assertEqual(key, duplicate_key)
        self.assertEqual(envelope["payload"], payload)
        self.assertEqual(envelope["source"], "mock")
        self.assertEqual(envelope["location_query"], "New York")
        self.assertTrue(envelope["ingested_at"].endswith("Z"))
        self.assertEqual(len(self.s3.objects), 1)
        self.assertEqual(len(self.s3.calls), 2)

    def test_weather_api_request_succeeds_and_retries_transient_response(self):
        payload = {"location": {"name": "Detroit"}, "current": {"temperature": -2}}
        responses = [FakeResponse(503, {}), FakeResponse(200, payload)]
        request_calls = []
        delays = []

        def fake_get(*args, **kwargs):
            request_calls.append((args, kwargs))
            return responses.pop(0)

        with patch.dict(os.environ, {"WEATHERSTACK_API_KEY": "test-key"}):
            actual, source = fetch_weather_payload(
                "Detroit", use_mock=False, request_get=fake_get, sleep=delays.append
            )

        self.assertEqual(actual, payload)
        self.assertEqual(source, "weatherstack")
        self.assertEqual(len(request_calls), 2)
        self.assertEqual(request_calls[0][1]["timeout"], (5, 30))
        self.assertEqual(request_calls[0][1]["params"]["query"], "Detroit")
        self.assertEqual(delays, [0.5])

    def test_weather_api_failure_retries_and_returns_safe_error(self):
        delays = []
        calls = []

        def failed_get(*args, **kwargs):
            calls.append(kwargs)
            raise requests.Timeout("secret-bearing request URL should not be logged")

        with patch.dict(os.environ, {"WEATHERSTACK_API_KEY": "test-key"}):
            with self.assertRaisesRegex(RuntimeError, "after 3 attempt"):
                fetch_weather_payload(
                    "Toronto", use_mock=False, request_get=failed_get, sleep=delays.append
                )

        self.assertEqual(len(calls), 3)
        self.assertEqual(delays, [0.5, 1.0])

    def test_validation_rejects_missing_or_invalid_required_fields(self):
        with self.assertRaises(ValueError):
            validate_ev_records([{"vehicle_id": "EV-1"}])
        with self.assertRaises(ValueError):
            validate_weather_payload({"current": {}})
        with self.assertRaises(ValueError):
            validate_weather_payload({"location": {"name": "X"}, "current": {"temperature": 100}})


if __name__ == "__main__":
    unittest.main()
