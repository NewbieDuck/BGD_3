from __future__ import annotations

import logging
import re

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import NullType
from sqlalchemy import create_engine, text

from src.config import load_config

logger = logging.getLogger(__name__)


SEASONS = [2018, 2019, 2021, 2022, 2023, 2024, 2025]
BRONZE_DIR = load_config().bronze_dir


def _engine_from_jdbc(jdbc_url: str, jdbc_props: dict):
    engine_url = jdbc_url.replace("jdbc:", "", 1)
    connect_args = {}

    if jdbc_props.get("user"):
        connect_args["user"] = jdbc_props["user"]
    if jdbc_props.get("password"):
        connect_args["password"] = jdbc_props["password"]

    return create_engine(engine_url, connect_args=connect_args)


def _bronze_prefix(bronze_table: str) -> str:
    return {
        "bronze_laps": "laps",
        "bronze_car_data": "tel",
        "bronze_weather": "weather",
    }[bronze_table]


def _normalize_bronze_columns(bronze_df, bronze_table: str):
    rename_map = {
        "Year": "year",
        "Round": "round",
        "SessionType": "session_type",
        "GrandPrix": "grand_prix",
    }

    if bronze_table == "bronze_car_data":
        rename_map["Driver"] = "driver"

    for source_name, target_name in rename_map.items():
        if source_name in bronze_df.columns and target_name not in bronze_df.columns:
            bronze_df = bronze_df.withColumnRenamed(source_name, target_name)

    return bronze_df


def _normalize_pandas_for_spark(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()

    for column in normalized.columns:
        series = normalized[column]
        if series.isna().all():
            normalized = normalized.drop(columns=[column])
            continue
        if pd.api.types.is_timedelta64_dtype(series):
            normalized[column] = series.dt.total_seconds()
        elif pd.api.types.is_datetime64_any_dtype(series):
            if getattr(series.dt, "tz", None) is not None:
                series = series.dt.tz_convert(None)
            normalized[column] = series.astype("datetime64[us]")

    return normalized


def _require_columns(df, table_name: str, required_columns: list[str]):
    """Add missing columns as NULL instead of raising error (for legacy data support)"""
    missing = [column_name for column_name in required_columns if column_name not in df.columns]
    if missing:
        logger.warning(f"{table_name} missing columns (adding as NULL): {', '.join(missing)}")
        for col_name in missing:
            df = df.withColumn(col_name, lit(None))
    return df


def _column_or_null(df, column_name: str):
    return col(column_name) if column_name in df.columns else lit(None).alias(column_name)


def _bronze_checkpoint_paths(bronze_table: str, year: int):
    prefix = _bronze_prefix(bronze_table)
    # collect both new-style files (with session type suffix) and legacy files
    paths_set = set()

    # files that include session type, e.g. laps_checkpoint_2025_r1_R.parquet
    for session_type in ("R", "Q"):
        for p in BRONZE_DIR.glob(f"{prefix}_checkpoint_{year}_r*_ {session_type}.parquet"):
            paths_set.add(p)

    # legacy files without session type, e.g. laps_checkpoint_2025_r1.parquet
    for p in BRONZE_DIR.glob(f"{prefix}_checkpoint_{year}_r*.parquet"):
        paths_set.add(p)

    # return sorted list for deterministic processing
    return sorted(paths_set)


def _discover_checkpoint_sessions(bronze_table: str, year: int):
    sessions = []

    # accept filenames with optional session suffix
    for path in _bronze_checkpoint_paths(bronze_table, year):
        match = re.search(r"_r(\d+)(?:_([RQ]))?\.parquet$", path.name)
        if match:
            round_num = int(match.group(1))
            session_type = match.group(2) or "R"  # default to 'R' for legacy files
            sessions.append((round_num, session_type))

    return sorted(set(sessions))


def _load_bronze_session(
    spark,
    bronze_table: str,
    year: int,
    round_num: int,
    session_type: str,
):
    prefix = _bronze_prefix(bronze_table)
    checkpoint_path = BRONZE_DIR / f"{prefix}_checkpoint_{year}_r{round_num}_{session_type}.parquet"

    if checkpoint_path.exists():
        try:
            bronze_slice = spark.read.parquet(str(checkpoint_path))
            bronze_slice.limit(1).count()
        except Exception as exc:
            err = str(exc)
            recoverable = (
                "PARQUET_TYPE_ILLEGAL" in err
                or "Illegal Parquet type" in err
                or "getSubject is not supported" in err
                or "Cannot load filesystem" in err
            )
            if not recoverable:
                raise

            df_pd = pd.read_parquet(checkpoint_path, engine="pyarrow")
            bronze_slice = spark.createDataFrame(_normalize_pandas_for_spark(df_pd))

        return _normalize_bronze_columns(bronze_slice, bronze_table)

    legacy_checkpoint_path = BRONZE_DIR / f"{prefix}_checkpoint_{year}_r{round_num}.parquet"
    if legacy_checkpoint_path.exists():
        df_pd = pd.read_parquet(legacy_checkpoint_path, engine="pyarrow")
        if "SessionType" not in df_pd.columns and "session_type" not in df_pd.columns:
            df_pd["SessionType"] = session_type
        bronze_slice = spark.createDataFrame(_normalize_pandas_for_spark(df_pd))
        return _normalize_bronze_columns(bronze_slice, bronze_table)

    raise FileNotFoundError(
        f"Missing bronze checkpoint for {year} round {round_num} session {session_type}: {checkpoint_path}"
    )


def _table_columns(conn, table_name: str) -> list[str]:
    rows = conn.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = :table_name
            ORDER BY ordinal_position
            """
        ),
        {"table_name": table_name},
    ).fetchall()
    return [row[0] for row in rows]


def _refresh_session_slice(
    df,
    engine,
    jdbc_url: str,
    jdbc_props: dict,
    target_table: str,
    stage_table: str,
    year: int,
    round_num: int,
    session_type: str,
) -> int:
    with engine.begin() as conn:
        target_columns = _table_columns(conn, target_table)
        if not target_columns:
            raise ValueError(f"Target table {target_table} does not exist. Run schema initialization first.")

    write_columns = [column_name for column_name in df.columns if column_name in target_columns]
    if not write_columns:
        raise ValueError(f"Brak wspolnych kolumn miedzy dataframe i tabela {target_table}")

    write_df = df.select(*write_columns)
    
    # filter out NullType columns (causes "Can't get JDBC type for void" error)
    null_type_cols = [f.name for f in write_df.schema.fields if isinstance(f.dataType, NullType)]
    if null_type_cols:
        write_df = write_df.drop(*null_type_cols)
        write_columns = [c for c in write_columns if c not in null_type_cols]
    
    record_count = write_df.count()

    if record_count > 0:
        write_df.write.jdbc(
            url=jdbc_url,
            table=stage_table,
            mode="overwrite",
            properties=jdbc_props,
        )

    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                DELETE FROM {target_table}
                WHERE "year" = :year
                  AND "round" = :round
                  AND session_type = :session_type
                """
            ),
            {"year": year, "round": round_num, "session_type": session_type},
        )

        if record_count > 0:
            cols_sql = ", ".join(f'"{column_name}"' for column_name in write_columns)
            conn.execute(
                text(
                    f"""
                    INSERT INTO {target_table} ({cols_sql})
                    SELECT {cols_sql}
                    FROM {stage_table}
                    """
                )
            )

        conn.execute(text(f"DROP TABLE IF EXISTS {stage_table}"))

    return record_count


def _sync_year_sessions(
    spark,
    jdbc_url,
    jdbc_props,
    engine,
    bronze_table: str,
    silver_table: str,
    year: int,
    transform_fn,
    rounds=None,
    session_types=None,
):
    sessions = _discover_checkpoint_sessions(bronze_table, year)
    selected_rounds = None if rounds is None else {int(round_num) for round_num in rounds}
    selected_session_types = None if session_types is None else set(session_types)

    session_pairs = sessions
    if selected_rounds is not None:
        session_pairs = [(round_num, session_type) for round_num, session_type in session_pairs if round_num in selected_rounds]
    if selected_session_types is not None:
        session_pairs = [(round_num, session_type) for round_num, session_type in session_pairs if session_type in selected_session_types]
    if not session_pairs:
        raise FileNotFoundError(f"No bronze sessions found for {bronze_table} year={year}")

    for round_num, session_type in session_pairs:
        bronze_slice = _load_bronze_session(
            spark,
            bronze_table,
            year,
            round_num,
            session_type,
        )

        df = transform_fn(bronze_slice, session_type)
        new_count = _refresh_session_slice(
            df,
            engine,
            jdbc_url,
            jdbc_props,
            target_table=silver_table,
            stage_table=f"tmp_{silver_table}",
            year=year,
            round_num=round_num,
            session_type=session_type,
        )

        print(f"[{year} R{round_num} {session_type}] zapisano {new_count} rekordow")


def _transform_laps(bronze_df, session_type: str):
    bronze_df = _require_columns(
        bronze_df,
        "silver_laps",
        [
            "year", "round", "grand_prix", "Driver", "DriverNumber",
            "Team", "LapNumber", "LapTime", "Sector1Time", "Sector2Time", "Sector3Time",
            "SpeedI1", "SpeedI2", "SpeedFL", "SpeedST", "Compound", "TyreLife",
            "FreshTyre", "IsAccurate", "TrackStatus", "Stint",
        ],
    )
    return (
        bronze_df
        .filter(col("LapTime").isNotNull())
        .filter(col("LapTime") > 0)
        .filter(col("Driver").isNotNull())
        .filter(col("IsAccurate") == True)
        .select(
            col("year"),
            col("round"),
            lit(session_type).alias("session_type"),
            col("grand_prix"),
            col("Driver").alias("driver"),
            col("DriverNumber").alias("driver_number"),
            col("Team").alias("team"),
            col("LapNumber").alias("lap_number"),
            col("LapTime").alias("lap_time_s"),
            col("Sector1Time").alias("sector1_s"),
            col("Sector2Time").alias("sector2_s"),
            col("Sector3Time").alias("sector3_s"),
            col("SpeedI1").alias("speed_i1"),
            col("SpeedI2").alias("speed_i2"),
            col("SpeedFL").alias("speed_fl"),
            col("SpeedST").alias("speed_st"),
            col("Compound").alias("tyre_compound"),
            col("TyreLife").alias("tyre_life_laps"),
            col("FreshTyre").alias("fresh_tyre"),
            col("IsAccurate").alias("is_accurate"),
            col("TrackStatus").alias("track_status"),
            _column_or_null(bronze_df, "Position").alias("position"),
            col("Stint").alias("stint"),
        )
    )


def _transform_car_data(bronze_df, session_type: str):
    bronze_df = _require_columns(
        bronze_df,
        "silver_car_data",
        [
            "year", "round", "grand_prix", "driver", "Date",
            "Speed", "RPM", "Throttle", "Brake", "DRS", "nGear",
        ],
    )
    return (
        bronze_df
        .filter(col("Speed").between(0, 400))
        .filter(col("RPM").between(0, 20000))
        .filter(col("Throttle").between(0, 100))
        .filter(col("driver").isNotNull())
        .select(
            col("year"),
            col("round"),
            lit(session_type).alias("session_type"),
            col("grand_prix"),
            col("driver"),
            col("Date").alias("recorded_at"),
            col("Speed").alias("speed_kmh"),
            col("RPM").alias("rpm"),
            col("Throttle").alias("throttle_pct"),
            col("Brake").alias("brake_on"),
            col("DRS").alias("drs_active"),
            col("nGear").alias("gear"),
        )
    )


def _transform_weather(bronze_df, session_type: str):
    bronze_df = _require_columns(
        bronze_df,
        "silver_weather",
        [
            "year", "round", "grand_prix", "Time", "AirTemp",
            "TrackTemp", "Humidity", "WindSpeed", "WindDirection", "Pressure", "Rainfall",
        ],
    )
    return (
        bronze_df
        .filter(col("AirTemp").between(-10, 60))
        .filter(col("Humidity").between(0, 100))
        .select(
            col("year"),
            col("round"),
            lit(session_type).alias("session_type"),
            col("grand_prix"),
            col("Time").alias("session_time"),
            col("AirTemp").alias("air_temp_c"),
            col("TrackTemp").alias("track_temp_c"),
            col("Humidity").alias("humidity_pct"),
            col("WindSpeed").alias("wind_speed_ms"),
            col("WindDirection").alias("wind_direction_deg"),
            col("Pressure").alias("pressure_hpa"),
            col("Rainfall").alias("is_raining"),
        )
    )


def run(spark: SparkSession, jdbc_url: str, jdbc_props: dict, seasons=None, rounds=None, session_types=None):
    print("\nSilver: start")
    engine = _engine_from_jdbc(jdbc_url, jdbc_props)
    selected_years = SEASONS if seasons is None else list(seasons)

    print("\n silver_laps...")
    for year in selected_years:
        _sync_year_sessions(
            spark,
            jdbc_url,
            jdbc_props,
            engine,
            bronze_table="bronze_laps",
            silver_table="silver_laps",
            year=year,
            transform_fn=_transform_laps,
            rounds=rounds,
            session_types=session_types,
        )

    print("\nPrzetwarzam silver_car_data")
    for year in selected_years:
        _sync_year_sessions(
            spark,
            jdbc_url,
            jdbc_props,
            engine,
            bronze_table="bronze_car_data",
            silver_table="silver_car_data",
            year=year,
            transform_fn=_transform_car_data,
            rounds=rounds,
            session_types=session_types,
        )

    print("\nPrzetwarzam silver_weather")
    for year in selected_years:
        _sync_year_sessions(
            spark,
            jdbc_url,
            jdbc_props,
            engine,
            bronze_table="bronze_weather",
            silver_table="silver_weather",
            year=year,
            transform_fn=_transform_weather,
            rounds=rounds,
            session_types=session_types,
        )
