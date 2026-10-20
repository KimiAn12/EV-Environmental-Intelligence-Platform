import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

sys.path.append(str(PROJECT_ROOT / "api-request"))

from api_request import configured_locations, mock_fetch_data

def test_configured_locations_defaults_to_operational_regions(monkeypatch):
    monkeypatch.delenv("WEATHER_LOCATIONS", raising=False)
    assert configured_locations() == ["New York", "Detroit", "Austin", "Fremont", "Toronto"]


def test_mock_fetch_data_matches_weatherstack_shape():
    payload = mock_fetch_data("Detroit")
    
    assert payload["location"]["name"] == "Detroit"
    assert "current" in payload
    assert isinstance(payload["current"]["weather_descriptions"], list)
    assert payload["current"]["temperature"] < 60
