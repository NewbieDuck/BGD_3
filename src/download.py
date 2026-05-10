import fastf1
import pandas as pd
import time
from datetime import date
import logging

from src.config import load_config

SEASONS = [2018, 2019, 2021, 2022, 2023, 2024, 2025]
SESSION_TYPES = ['R', 'Q']  # Wyścigi (R) i Kwalifikacje (Q)
CONFIG = load_config()
OUTPUT_DIR = CONFIG.bronze_dir
CACHE_DIR = CONFIG.fastf1_cache_dir


RESUME_YEAR = 2024

OUTPUT_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)
fastf1.Cache.enable_cache(str(CACHE_DIR))



logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def _normalize_for_spark_parquet(df: pd.DataFrame) -> pd.DataFrame:
    normalized = df.copy()

    for column in normalized.columns:
        series = normalized[column]
        if pd.api.types.is_timedelta64_dtype(series):
            normalized[column] = series.dt.total_seconds()
        elif pd.api.types.is_datetime64_any_dtype(series):
            if getattr(series.dt, "tz", None) is not None:
                series = series.dt.tz_convert(None)
            normalized[column] = series.astype("datetime64[us]")

    return normalized


def _write_spark_parquet(df: pd.DataFrame, path) -> None:
    normalized = _normalize_for_spark_parquet(df)
    normalized.to_parquet(
        path,
        engine="pyarrow",
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
        index=False,
    )


def _write_session_checkpoint(laps, telemetry, weather, out_dir, year, rnd, session_type):
    tag = f"checkpoint_{year}_r{rnd}_{session_type}"

    if laps:
        _write_spark_parquet(pd.concat(laps, ignore_index=True), out_dir / f"laps_{tag}.parquet")
    if telemetry:
        _write_spark_parquet(pd.concat(telemetry, ignore_index=True), out_dir / f"tel_{tag}.parquet")
    if weather:
        _write_spark_parquet(pd.concat(weather, ignore_index=True), out_dir / f"weather_{tag}.parquet")

def run_bronze(season=None, resume_year=None, rounds=None, session_types=None):
    if season is None:
        season = SEASONS
    if resume_year is None:
        resume_year = RESUME_YEAR
    selected_rounds = None if rounds is None else {int(round_num) for round_num in rounds}
    selected_session_types = SESSION_TYPES if session_types is None else list(session_types)
    
    for year in season:

        if year < resume_year:
            logger.info(f"Pomijam sezon {year} (juz pobrany)")
            continue

        logger.info(f"\n{'=' * 50}\n  Sezon {year}\n{'=' * 50}")
        schedule = fastf1.get_event_schedule(year, include_testing=False)

        for _, event in schedule.iterrows():
            round_num = event['RoundNumber']
            gp_name = event['EventName']

            if selected_rounds is not None and int(round_num) not in selected_rounds:
                continue

            if event['EventDate'].date() > date.today():
                logger.info(f"  [{year}] Runda {round_num}: {gp_name} (przyszly)")
                continue

            # Przetwarzaj zarówno R (wyścig) jak i Q (kwalifikacje)
            for session_type in selected_session_types:
                tag = f"{year}_r{round_num}_{session_type}"
                path_to_check_laps = OUTPUT_DIR / f"laps_checkpoint_{tag}.parquet"
                path_to_check_weather = OUTPUT_DIR / f"weather_checkpoint_{tag}.parquet"
                path_to_check_telemetry = OUTPUT_DIR / f"tel_checkpoint_{tag}.parquet"

                if path_to_check_laps.exists() and path_to_check_weather.exists() and path_to_check_telemetry.exists():
                    logger.info(f"  [{year}] R{round_num} {gp_name} [{session_type}] (juz pobrany)")
                    continue

                logger.info(f"  [{year}] R{round_num} {gp_name} [{session_type}]")

                try:
                    session = fastf1.get_session(year, round_num, session_type)
                    session.load(telemetry=True, weather=True, messages=False)

                    session_laps = []
                    session_telemetry = []
                    session_weather = []

                    laps = session.laps.copy()
                    laps['Year'] = year
                    laps['Round'] = round_num
                    laps['SessionType'] = session_type
                    laps['GrandPrix'] = gp_name
                    session_laps.append(laps)

                    for driver in session.drivers:
                        try:
                            driver_laps = session.laps.pick_drivers(driver)
                            car_data = driver_laps.get_car_data()
                            car_data['Year'] = year
                            car_data['Round'] = round_num
                            car_data['SessionType'] = session_type
                            car_data['GrandPrix'] = gp_name
                            car_data['Driver'] = driver
                            session_telemetry.append(car_data)
                        except Exception:
                            pass

                    weather = session.weather_data.copy()
                    weather['Year'] = year
                    weather['Round'] = round_num
                    weather['SessionType'] = session_type
                    weather['GrandPrix'] = gp_name
                    session_weather.append(weather)

                    _write_session_checkpoint(
                        session_laps,
                        session_telemetry,
                        session_weather,
                        OUTPUT_DIR,
                        year,
                        round_num,
                        session_type,
                    )

                    logger.info(f"ok ({len(laps)} okr.)")

                    time.sleep(8)

                except Exception as e:
                    logger.exception(f"Exception: {e}")
                    if "500 calls/h" in str(e):
                        logger.warning("Rate limit, czekam 10 minut...")
                        time.sleep(600)
                    continue

    report = {
        "laps_count": 0,
        "telemetry_count": 0,
        "weather_count": 0,
        "status": "Success"
    }
    logger.info("\nGotowe!")
    return report

