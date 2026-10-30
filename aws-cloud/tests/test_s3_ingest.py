import json
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingestion.s3_ingest import (  # noqa: E402
    upload_ev_file,
    upload_weather_payload,
    validate_ev_records,
    validate_weather_payload,
)


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, **kwargs):
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs


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
        uploaded = next(iter(self.s3.objects.values()))
        self.assertEqual(uploaded["ContentType"], "application/x-ndjson")
        self.assertEqual(json.loads(uploaded["Body"].decode().splitlines()[0])["ingest_date"], "2026-10-30")

    def test_weather_upload_preserves_payload_in_json_envelope(self):
        payload = {"location": {"name": "New York"}, "current": {"temperature": 15}}
        key = upload_weather_payload(self.s3, "example-bucket", "New York", payload, "2026-10-30", "mock")
        uploaded = next(iter(self.s3.objects.values()))
        envelope = json.loads(uploaded["Body"])

        self.assertIn("raw/weather/ingest_date=2026-10-30/location=new-york/", key)
        self.assertEqual(envelope["payload"], payload)
        self.assertEqual(envelope["source"], "mock")

    def test_validation_rejects_missing_or_invalid_required_fields(self):
        with self.assertRaises(ValueError):
            validate_ev_records([{"vehicle_id": "EV-1"}])
        with self.assertRaises(ValueError):
            validate_weather_payload({"current": {}})


if __name__ == "__main__":
    unittest.main()
