CREATE SCHEMA IF NOT EXISTS powerbi;

CREATE OR REPLACE VIEW powerbi.ev_environmental_performance AS
WITH latest_weather AS (
    SELECT
        payload->'location'->>'name' AS city,
        payload->'location'->>'country' AS country,
        payload->'location'->>'region' AS state_or_region,
        NULLIF(payload->'location'->>'localtime', '')::timestamp AS weather_time_local,
        DATE(NULLIF(payload->'location'->>'localtime', '')::timestamp) AS weather_date,
        NULLIF(payload->'current'->>'temperature', '')::numeric AS temperature_c,
        payload->'current'->'weather_descriptions'->>0 AS weather_description,
        NULLIF(payload->'current'->>'wind_speed', '')::numeric AS wind_speed_kph,
        NULLIF(payload->'current'->>'humidity', '')::numeric AS humidity_pct,
        NULLIF(payload->'current'->>'precip', '')::numeric AS precipitation_mm,
        fetched_at,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(payload->'location'->>'name')
            ORDER BY fetched_at DESC
        ) AS rn
    FROM raw.weather_api_responses
),
ev AS (
    SELECT
        vehicle_id,
        model,
        region,
        city,
        battery_kwh,
        rated_efficiency_kwh_per_100km,
        observed_efficiency_kwh_per_100km,
        observed_efficiency_kwh_per_100km - rated_efficiency_kwh_per_100km AS efficiency_delta_kwh_per_100km,
        ROUND(
            ((observed_efficiency_kwh_per_100km / NULLIF(rated_efficiency_kwh_per_100km, 0)) - 1) * 100,
            2
        ) AS efficiency_penalty_pct,
        ROUND((battery_kwh / NULLIF(observed_efficiency_kwh_per_100km, 0)) * 100, 1) AS estimated_range_km,
        odometer_km,
        operating_date,
        loaded_at
    FROM raw.ev_fleet_records
)
SELECT
    ev.vehicle_id,
    ev.model,
    ev.region,
    ev.city,
    ev.battery_kwh,
    ev.rated_efficiency_kwh_per_100km,
    ev.observed_efficiency_kwh_per_100km,
    ev.efficiency_delta_kwh_per_100km,
    ev.efficiency_penalty_pct,
    ev.estimated_range_km,
    ev.odometer_km,
    ev.operating_date,
    ev.loaded_at,
    latest_weather.country,
    latest_weather.state_or_region,
    latest_weather.weather_time_local,
    latest_weather.weather_date,
    latest_weather.temperature_c,
    latest_weather.weather_description,
    latest_weather.wind_speed_kph,
    latest_weather.humidity_pct,
    latest_weather.precipitation_mm,
    CASE
        WHEN latest_weather.temperature_c < 0 THEN 'freezing'
        WHEN latest_weather.temperature_c < 10 THEN 'cold'
        WHEN latest_weather.temperature_c <= 25 THEN 'mild'
        WHEN latest_weather.temperature_c IS NULL THEN 'unknown'
        ELSE 'hot'
    END AS temperature_band,
    CASE
        WHEN latest_weather.city IS NULL THEN 'missing_weather'
        WHEN latest_weather.temperature_c < 0 THEN 'high_cold_weather_risk'
        WHEN latest_weather.temperature_c > 30 THEN 'high_heat_weather_risk'
        WHEN ev.efficiency_penalty_pct >= 25 THEN 'high_efficiency_penalty'
        WHEN ev.efficiency_penalty_pct >= 10 THEN 'moderate_efficiency_penalty'
        ELSE 'normal'
    END AS operational_risk_label,
    CASE
        WHEN latest_weather.city IS NULL THEN 0
        ELSE 1
    END AS has_weather_match
FROM ev
LEFT JOIN latest_weather
    ON LOWER(ev.city) = LOWER(latest_weather.city)
    AND latest_weather.rn = 1;

CREATE OR REPLACE VIEW powerbi.region_environment_summary AS
SELECT
    region,
    city,
    operating_date,
    COUNT(DISTINCT vehicle_id) AS vehicle_count,
    ROUND(AVG(temperature_c), 2) AS avg_temperature_c,
    ROUND(AVG(observed_efficiency_kwh_per_100km), 2) AS avg_observed_efficiency_kwh_per_100km,
    ROUND(AVG(rated_efficiency_kwh_per_100km), 2) AS avg_rated_efficiency_kwh_per_100km,
    ROUND(AVG(efficiency_delta_kwh_per_100km), 2) AS avg_efficiency_delta_kwh_per_100km,
    ROUND(AVG(efficiency_penalty_pct), 2) AS avg_efficiency_penalty_pct,
    ROUND(AVG(estimated_range_km), 1) AS avg_estimated_range_km,
    SUM(CASE WHEN operational_risk_label <> 'normal' THEN 1 ELSE 0 END) AS elevated_risk_vehicle_count,
    ROUND(AVG(has_weather_match) * 100, 1) AS weather_match_rate_pct
FROM powerbi.ev_environmental_performance
GROUP BY
    region,
    city,
    operating_date;

CREATE OR REPLACE VIEW powerbi.charging_planning_signals AS
SELECT
    region,
    city,
    operating_date,
    vehicle_count,
    avg_temperature_c,
    avg_efficiency_penalty_pct,
    avg_estimated_range_km,
    elevated_risk_vehicle_count,
    weather_match_rate_pct,
    CASE
        WHEN avg_efficiency_penalty_pct >= 25 OR avg_estimated_range_km < 260 THEN 'prioritize_charging_capacity'
        WHEN avg_efficiency_penalty_pct >= 10 THEN 'monitor_capacity'
        ELSE 'normal_capacity'
    END AS charging_planning_signal
FROM powerbi.region_environment_summary;

CREATE OR REPLACE VIEW powerbi.data_quality_latest AS
WITH ranked AS (
    SELECT
        check_name,
        layer_name,
        table_name,
        status,
        failed_count,
        details,
        checked_at,
        ROW_NUMBER() OVER (
            PARTITION BY check_name
            ORDER BY checked_at DESC
        ) AS rn
    FROM validation.data_quality_results
)
SELECT
    check_name,
    layer_name,
    table_name,
    status,
    failed_count,
    details,
    checked_at
FROM ranked
WHERE rn = 1;

CREATE OR REPLACE VIEW powerbi.refresh_summary AS
SELECT
    (SELECT COUNT(*) FROM raw.weather_api_responses) AS weather_payload_count,
    (SELECT MAX(fetched_at) FROM raw.weather_api_responses) AS latest_weather_fetch_at,
    (SELECT COUNT(*) FROM raw.ev_fleet_records) AS ev_record_count,
    (SELECT MAX(loaded_at) FROM raw.ev_fleet_records) AS latest_ev_load_at,
    (SELECT COUNT(*) FROM validation.data_quality_results WHERE status = 'fail') AS validation_failure_count,
    CURRENT_TIMESTAMP AS dashboard_refreshed_at;
