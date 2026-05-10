from __future__ import annotations

from sqlalchemy import create_engine, text


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS silver_laps (
    "year" INT NOT NULL,
    "round" INT NOT NULL,
    session_type TEXT NOT NULL,
    grand_prix TEXT,
    driver TEXT NOT NULL,
    driver_number TEXT,
    team TEXT,
    lap_number DOUBLE PRECISION NOT NULL,
    lap_time_s DOUBLE PRECISION,
    sector1_s DOUBLE PRECISION,
    sector2_s DOUBLE PRECISION,
    sector3_s DOUBLE PRECISION,
    speed_i1 DOUBLE PRECISION,
    speed_i2 DOUBLE PRECISION,
    speed_fl DOUBLE PRECISION,
    speed_st DOUBLE PRECISION,
    tyre_compound TEXT,
    tyre_life_laps DOUBLE PRECISION,
    fresh_tyre BOOLEAN,
    is_accurate BOOLEAN,
    track_status TEXT,
    position DOUBLE PRECISION,
    stint DOUBLE PRECISION,
    PRIMARY KEY ("year", "round", session_type, driver, lap_number)
);

CREATE TABLE IF NOT EXISTS silver_car_data (
    "year" INT NOT NULL,
    "round" INT NOT NULL,
    session_type TEXT NOT NULL,
    grand_prix TEXT,
    driver TEXT NOT NULL,
    recorded_at TIMESTAMP NOT NULL,
    speed_kmh DOUBLE PRECISION,
    rpm DOUBLE PRECISION,
    throttle_pct DOUBLE PRECISION,
    brake_on BOOLEAN,
    drs_active INT,
    gear INT,
    PRIMARY KEY ("year", "round", session_type, driver, recorded_at)
);

CREATE TABLE IF NOT EXISTS silver_weather (
    "year" INT NOT NULL,
    "round" INT NOT NULL,
    session_type TEXT NOT NULL,
    grand_prix TEXT,
    session_time DOUBLE PRECISION NOT NULL,
    air_temp_c DOUBLE PRECISION,
    track_temp_c DOUBLE PRECISION,
    humidity_pct DOUBLE PRECISION,
    wind_speed_ms DOUBLE PRECISION,
    wind_direction_deg INT,
    pressure_hpa DOUBLE PRECISION,
    is_raining BOOLEAN,
    PRIMARY KEY ("year", "round", session_type, session_time)
);

CREATE TABLE IF NOT EXISTS gold_driver_performance (
    "year" INT NOT NULL,
    grand_prix TEXT NOT NULL,
    session_type TEXT NOT NULL,
    driver TEXT NOT NULL,
    team TEXT NOT NULL,
    total_laps BIGINT,
    avg_lap_time_s DOUBLE PRECISION,
    best_lap_time_s DOUBLE PRECISION,
    avg_sector1_s DOUBLE PRECISION,
    avg_sector2_s DOUBLE PRECISION,
    avg_sector3_s DOUBLE PRECISION,
    avg_speed_trap_kmh DOUBLE PRECISION,
    max_speed_trap_kmh DOUBLE PRECISION,
    soft_laps BIGINT,
    medium_laps BIGINT,
    hard_laps BIGINT,
    rank_in_race INT,
    PRIMARY KEY ("year", grand_prix, session_type, driver, team)
);

CREATE TABLE IF NOT EXISTS gold_team_season_stats (
    "year" INT NOT NULL,
    session_type TEXT NOT NULL,
    team TEXT NOT NULL,
    avg_lap_time_s DOUBLE PRECISION,
    best_lap_time_s DOUBLE PRECISION,
    total_laps BIGINT,
    drivers_count BIGINT,
    avg_speed_trap_kmh DOUBLE PRECISION,
    team_rank_in_season INT,
    PRIMARY KEY ("year", session_type, team)
);
"""

MIGRATION_SQL = """
ALTER TABLE silver_laps ADD COLUMN IF NOT EXISTS session_type TEXT;
ALTER TABLE silver_car_data ADD COLUMN IF NOT EXISTS session_type TEXT;
ALTER TABLE silver_weather ADD COLUMN IF NOT EXISTS session_type TEXT;
ALTER TABLE gold_driver_performance ADD COLUMN IF NOT EXISTS session_type TEXT;
ALTER TABLE gold_team_season_stats ADD COLUMN IF NOT EXISTS session_type TEXT;
"""


def ensure_schema(sqlalchemy_url: str) -> None:
    engine = create_engine(sqlalchemy_url)
    with engine.begin() as conn:
        for statement in f"{SCHEMA_SQL}\n{MIGRATION_SQL}".split(";"):
            if statement.strip():
                conn.execute(text(statement))
