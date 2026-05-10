from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from sqlalchemy import create_engine, text


def _engine_from_jdbc(jdbc_url: str, jdbc_props: dict):
    engine_url = jdbc_url.replace("jdbc:", "", 1)
    connect_args = {}

    if jdbc_props.get("user"):
        connect_args["user"] = jdbc_props["user"]
    if jdbc_props.get("password"):
        connect_args["password"] = jdbc_props["password"]

    return create_engine(engine_url, connect_args=connect_args)


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


def _refresh_table_from_stage(
    df,
    engine,
    jdbc_url: str,
    jdbc_props: dict,
    target_table: str,
    stage_table: str,
    key_columns: list[str],
) -> int:
    record_count = df.count()
    if record_count == 0:
        return 0

    with engine.begin() as conn:
        target_columns = _table_columns(conn, target_table)
        if not target_columns:
            raise ValueError(f"Target table {target_table} does not exist. Run schema initialization first.")

    write_columns = [column_name for column_name in df.columns if column_name in target_columns]
    df.select(*write_columns).write.jdbc(
        url=jdbc_url,
        table=stage_table,
        mode="overwrite",
        properties=jdbc_props,
    )

    join_condition = " AND ".join(
        [f'target."{column}" = stage."{column}"' for column in key_columns]
    )
    cols_sql = ", ".join(f'"{column_name}"' for column_name in write_columns)

    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                DELETE FROM {target_table} AS target
                USING {stage_table} AS stage
                WHERE {join_condition}
                """
            )
        )
        conn.execute(
            text(
                f"""
                INSERT INTO {target_table} ({cols_sql})
                SELECT {cols_sql}
                FROM {stage_table}
                """
            )
        )
        conn.execute(text(f'DROP TABLE IF EXISTS {stage_table}'))

    return record_count


def run(spark: SparkSession, jdbc_url: str, jdbc_props: dict):
    print("\nGold: start")

    engine = _engine_from_jdbc(jdbc_url, jdbc_props)

    silver_laps = spark.read.jdbc(
        url=jdbc_url,
        table="silver_laps",
        properties=jdbc_props
    )

    # ===== GOLD DRIVER =====
    window_driver = (
        Window
        .partitionBy("year", "grand_prix", "session_type")
        .orderBy("best_lap_time_s")
    )

    gold_driver = (
        silver_laps
        .groupBy("year", "grand_prix", "session_type", "driver", "team")
        .agg(
            F.countDistinct("lap_number").alias("total_laps"),
            F.round(F.avg("lap_time_s"), 3).alias("avg_lap_time_s"),
            F.round(F.min("lap_time_s"), 3).alias("best_lap_time_s"),
            F.round(F.avg("sector1_s"), 3).alias("avg_sector1_s"),
            F.round(F.avg("sector2_s"), 3).alias("avg_sector2_s"),
            F.round(F.avg("sector3_s"), 3).alias("avg_sector3_s"),
            F.round(F.avg("speed_st"), 1).alias("avg_speed_trap_kmh"),
            F.round(F.max("speed_st"), 1).alias("max_speed_trap_kmh"),
            F.count(F.when(F.col("tyre_compound") == "SOFT",   1)).alias("soft_laps"),
            F.count(F.when(F.col("tyre_compound") == "MEDIUM", 1)).alias("medium_laps"),
            F.count(F.when(F.col("tyre_compound") == "HARD",   1)).alias("hard_laps"),
        )
        .withColumn("rank_in_race", F.rank().over(window_driver))
    )

    driver_rows = _refresh_table_from_stage(
        gold_driver,
        engine,
        jdbc_url,
        jdbc_props,
        target_table="gold_driver_performance",
        stage_table="tmp_gold_driver_performance",
        key_columns=["year", "grand_prix", "session_type", "driver", "team"],
    )
    print(f"  gold_driver_performance: {driver_rows} rekordow")

    # ===== GOLD TEAM (analogicznie) =====
    window_team = (
        Window
        .partitionBy("year", "session_type")
        .orderBy("avg_lap_time_s")
    )

    gold_team = (
        silver_laps
        .groupBy("year", "session_type", "team")
        .agg(
            F.round(F.avg("lap_time_s"), 3).alias("avg_lap_time_s"),
            F.round(F.min("lap_time_s"), 3).alias("best_lap_time_s"),
            F.countDistinct("lap_number").alias("total_laps"),
            F.countDistinct("driver").alias("drivers_count"),
            F.round(F.avg("speed_st"), 1).alias("avg_speed_trap_kmh"),
        )
        .withColumn("team_rank_in_season", F.rank().over(window_team))
    )

    team_rows = _refresh_table_from_stage(
        gold_team,
        engine,
        jdbc_url,
        jdbc_props,
        target_table="gold_team_season_stats",
        stage_table="tmp_gold_team_season_stats",
        key_columns=["year", "session_type", "team"],
    )
    print(f"gold_team_season_stats: {team_rows} rekordow")
