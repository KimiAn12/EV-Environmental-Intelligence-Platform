import os
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

WEATHERSTACK_URL = "https://api.weatherstack.com/current"


def configured_locations():
    raw_locations = os.getenv("WEATHER_LOCATIONS", "New York,Detroit,Austin,Fremont,Toronto")
    return [location.strip() for location in raw_locations.split(",") if location.strip()]


def fetch_data(location):
    api_key = os.getenv("WEATHERSTACK_API_KEY", "")
    if not api_key:
        raise ValueError("WEATHERSTACK_API_KEY is required when USE_MOCK_WEATHER=false")
    response = requests.get(
        WEATHERSTACK_URL,
        params={"access_key": api_key, "query": location},
        timeout=30,
    )

    response.raise_for_status()
    payload = response.json()

    if payload.get("success") is False:
        raise ValueError(f"Weatherstack error for {location}: {payload}")
    return payload


def mock_fetch_data(location="New York"):
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

    localtime = datetime.utcnow().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

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


def get_weather_payload(location):
    use_mock = os.getenv("USE_MOCK_WEATHER", "true").lower() in {"1", "true", "yes"}
    if use_mock:
        return mock_fetch_data(location)
    return fetch_data(location)
