from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

from pyspark.sql import SparkSession

from src.config import load_config
from src.download import run_bronze
from src.gold import run as run_gold
from src.schema import ensure_schema
from src.silver import run as run_silver


logger = logging.getLogger(__name__)

CONFIG = load_config()
JDBC_URL = CONFIG.jdbc_url
JDBC_PROPS = CONFIG.jdbc_props
JDBC_JAR = CONFIG.jdbc_jar


@dataclass(frozen=True)
class StageResult:
    name: str
    status: str
    attempts: int
    message: str


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("F1-Orchestrated-ELT-Pipeline")
        .config("spark.driver.extraClassPath", str(JDBC_JAR))
        .config("spark.executor.extraClassPath", str(JDBC_JAR))
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.maxResultSize", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def _run_stage(name: str, action: Callable[[], object], retries: int = 1) -> StageResult:
    last_error: Exception | None = None

    for attempt in range(1, retries + 2):
        try:
            logger.info("[%s] start (attempt %s/%s)", name, attempt, retries + 1)
            result = action()
            message = "ok" if result is None else str(result)
            logger.info("[%s] success", name)
            return StageResult(name=name, status="success", attempts=attempt, message=message)
        except Exception as exc:  # noqa: BLE001 - pipeline should capture stage failures
            last_error = exc
            logger.exception("[%s] failed on attempt %s/%s", name, attempt, retries + 1)
            if attempt <= retries:
                logger.info("[%s] retrying", name)

    assert last_error is not None
    return StageResult(name=name, status="failed", attempts=retries + 1, message=str(last_error))


def run_pipeline(
    smoke: bool = False,
    smoke_year: int = 2025,
    smoke_round: int | None = None,
    smoke_session: str | None = None,
) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    logger.info("=" * 60)
    logger.info("  F1 ELT Pipeline - START")
    logger.info("=" * 60)

    start_time = datetime.now()
    stage_results: list[StageResult] = []

    bronze_seasons = [smoke_year] if smoke else None
    silver_seasons = [smoke_year] if smoke else None
    bronze_resume_year = smoke_year if smoke else None
    selected_rounds = [smoke_round] if smoke and smoke_round is not None else None
    selected_session_types = [smoke_session] if smoke and smoke_session is not None else None

    ensure_schema(CONFIG.sqlalchemy_url)

    stage_results.append(
        _run_stage(
            "bronze",
            lambda: run_bronze(
                season=bronze_seasons,
                resume_year=bronze_resume_year,
                rounds=selected_rounds,
                session_types=selected_session_types,
            ),
            retries=1,
        )
    )
    if stage_results[-1].status != "success":
        logger.error("bronze stage failed: %s", stage_results[-1].message)
        return 1

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        stage_results.append(
            _run_stage(
                "silver",
                lambda: run_silver(
                    spark,
                    JDBC_URL,
                    JDBC_PROPS,
                    seasons=silver_seasons,
                    rounds=selected_rounds,
                    session_types=selected_session_types,
                ),
                retries=0,
            )
        )
        if stage_results[-1].status != "success":
            logger.error("silver stage failed: %s", stage_results[-1].message)
            return 1

        stage_results.append(_run_stage("gold", lambda: run_gold(spark, JDBC_URL, JDBC_PROPS), retries=0))
        if stage_results[-1].status != "success":
            logger.error("gold stage failed: %s", stage_results[-1].message)
            return 1
    finally:
        spark.stop()

    elapsed = datetime.now() - start_time
    logger.info("=" * 60)
    logger.info("  F1 ELT Pipeline - SUCCESS (czas: %s)", elapsed)
    logger.info("=" * 60)

    for result in stage_results:
        logger.info("stage=%s status=%s attempts=%s", result.name, result.status, result.attempts)

    return 0
