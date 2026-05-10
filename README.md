# F1 ELT Pipeline

A production-grade ELT (Extract, Load, Transform) pipeline for Formula 1 historical data using FastF1 as the data source. The pipeline follows the medallion architecture pattern (Bronze → Silver → Gold) with atomic, idempotent operations and comprehensive error handling.

**Status:** ✅ Production Ready | All stages tested and validated

---

## 📊 Overview

This pipeline automates the ingestion, transformation, and aggregation of Formula 1 data from 2018-2025 into PostgreSQL for analytics and BI use cases.

```
FastF1 API → Bronze (Parquet Checkpoints) → Silver (Transformation) → Gold (Aggregations)
                                                      ↓                       ↓
                                            PostgreSQL Tables         Analytics Ready
```



---

## 🏗️ Architecture

### Data Layers

| Layer | Location | Purpose |
|-------|----------|---------|
| **Bronze** | `f1_raw_data/*.parquet` | Raw FastF1 checkpoints (laps, car telemetry, weather) per season/round/session |
| **Silver** | PostgreSQL `silver_*` | Cleaned, deduplicated per-session data with transactional refresh |
| **Gold** | PostgreSQL `gold_*` | Pre-aggregated driver and team statistics for fast queries |

### Processing Stages

1. **Bronze Stage** (`src/download.py`):
   - Fetches sessions from FastF1 API
   - Writes checkpoint Parquet files (idempotent)
   - Supports resumable downloads

2. **Silver Stage** (`src/silver.py`):
   - Reads Bronze checkpoints via Spark
   - Validates and normalizes schemas
   - Performs per-session atomic refresh via staging tables
   - Writes to PostgreSQL `silver_laps`, `silver_car_data`, `silver_weather`

3. **Gold Stage** (`src/gold.py`):
   - Aggregates Silver data (driver performance, team stats)
   - Writes to PostgreSQL `gold_driver_performance`, `gold_team_season_stats`

### Orchestration

- **Control Plane:** `src/orchestrator.py` + `pipeline.py`
  - Stages run sequentially
  - Each stage retries independently (2x Bronze, 1x Silver/Gold)
  - Failures logged but don't cascade

---

## 🎯 System Requirements

### Minimum
- **Python:** 3.11+
- **PostgreSQL:** 12+ (must be running, user must exist with DB permissions)
- **Java:** 17+ (OpenJDK or Temurin)
- **Disk:** 10+ GB for data (Bronze + PostgreSQL)
- **RAM:** 8+ GB recommended



---

## 🚀 Installation & Setup

### Step 1: Clone the Repository

### Step 2: Install Dependencies

```bash
# Create virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install Python packages
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

**Dependencies:**
- `fastf1` — F1 data fetching
- `pyspark` — Distributed data processing
- `pandas`, `pyarrow` — Data manipulation
- `sqlalchemy`, `psycopg2-binary` — PostgreSQL connectivity

### Step 3: Set Up PostgreSQL

#### Create Database & User (as superuser)

```bash
psql -U postgres

-- Then in psql shell:
CREATE DATABASE f1db;
CREATE USER f1user WITH PASSWORD 'admin';
GRANT ALL PRIVILEGES ON DATABASE f1db TO f1user;
\q
```

#### Verify Connection

```bash
psql -h localhost -U f1user -d f1db -c "SELECT version();"
```

### Step 4: Configure Environment

1. Copy the example config:
```bash
cp .env.example .env
```

2. Edit `.env` with your PostgreSQL credentials and Java path:

```bash
# PostgreSQL Connection
F1_DB_HOST=localhost
F1_DB_PORT=5432
F1_DB_NAME=f1db
F1_DB_USER=f1user
F1_DB_PASSWORD=admin

# Java Runtime (find with: which java)
JAVA_HOME=/usr/lib/jvm/java-17-temurin-jdk
```

#### Finding Java Path

**Linux:**
```bash
which java                    # Output: /usr/bin/java
readlink -f /usr/bin/java     # Shows full JDK path
# Or:
ls -la /usr/lib/jvm/java-17*
```
---

## ▶️ Running the Pipeline

### Quick Start (Smoke Test)

Test the entire pipeline with a single session in ~90 seconds:

```bash
python pipeline.py --smoke --smoke-year 2025 --smoke-round 1 --smoke-session R
```

**Expected Output:**
```
[bronze] success
[silver] success — [2025 R1 R]:  x rekordow
[gold] success — gold_driver_performance: x rekordow
F1 ELT Pipeline - SUCCESS (time: 0:01:24)
Exit Code: 0
```

### Full Pipeline (All Seasons 2018-2025)

Download and process all historical data (~5-8 hours):

```bash
python pipeline.py
```

### Resume Mode (Continue from Last Failure)

If the pipeline was interrupted:

```bash
python pipeline.py --resume
```

### View All Options

```bash
python pipeline.py --help
```

---

## 📈 Data Schema

### Silver Tables

#### `silver_laps`
- Per-lap telemetry for each session
- **Key:** `(year, round, session_type, driver, lap_number)`
- Columns: lap_time, sector times, tire compound, position, stint, track_status

#### `silver_car_data`
- Driver input telemetry (throttle, brake, DRS, gear, speed)
- **Key:** `(year, round, session_type, driver, recorded_at)`
- Columns: speed, throttle, brake, drs, gear, rpms

#### `silver_weather`
- Track conditions per session
- **Key:** `(year, round, session_type, session_time)`
- Columns: air_temp, track_temp, wind_direction, humidity, rainfall

### Gold Tables

#### `gold_driver_performance`
- Per-session driver stats
- Columns: avg_lap_time, best_lap_rank, points, team, fastest_lap_flag

#### `gold_team_season_stats`
- Per-team season aggregates
- Columns: total_points, driver_count, avg_pace, dnf_count

---

## 🔍 Verification & Troubleshooting

### Check Database

```bash
psql -U f1user -d f1db -c "SELECT COUNT(*) as record_count FROM silver_laps;"
psql -U f1user -d f1db -c "\dt silver_* gold_*"  # List tables
```

### Common Issues

| Issue | Solution |
|-------|----------|
| `psycopg2.OperationalError: fe_sendauth: no password supplied` | Check `.env` — ensure `F1_DB_PASSWORD` is set |
| `java.lang.UnsupportedOperationException: getSubject is not supported` | Set `JAVA_HOME` in `.env`; try `java -version` first |
| `PARQUET_TYPE_ILLEGAL` warnings | Expected — pipeline falls back to Pandas; data still written |
| `Permission denied` on drivers/ | Run `chmod +x drivers/*` if needed |
| Database collation mismatch warning | Safe to ignore (PostgreSQL version issue, doesn't affect data) |

### Enable Debug Logging

```bash
# Set log level in orchestrator.py:
logging.basicConfig(level=logging.DEBUG)

python pipeline.py --smoke
```

---

## 📋 Project Structure

```
f1-elt-pipeline/
├── pipeline.py                 # CLI entry point
├── src/
│   ├── orchestrator.py        # Stage sequencing & retries
│   ├── download.py            # Bronze: FastF1 → Parquet
│   ├── silver.py              # Silver: Transform & Load
│   ├── gold.py                # Gold: Aggregations
│   ├── config.py              # Config loading (.env)
│   └── schema.py              # SQL table schemas
├── drivers/
│   └── postgresql-42.7.3.jar  # JDBC driver for Spark
├── diagramy/
│   ├── pipeline_architecture.md  # Architecture diagrams
│   └── ERD.md                    # Entity relationship diagram
├── requirements.txt            # Python dependencies
├── .env.example               # Environment template
├── .gitignore                 # Git ignore rules
└── README.md                  # This file
```

---

## 🔄 Idempotency & Recovery

### Bronze Stage
- Skips sessions with complete Parquet triples (laps + telemetry + weather)
- Safe to re-run; no duplicates

### Silver Stage
- Per-session atomic refresh: old records DELETE'd and new INSERT'd within transaction
- If interrupted mid-transaction, automatic rollback on next run
- Handles schema evolution (missing columns detected and added)

### Gold Stage
- Rebuilds aggregates from current Silver tables
- Can be safely re-run

**Bottom line:** It's safe to interrupt and restart at any point.

---

## 📊 Analytics Examples

### Query: Driver Performance (2025 Australian GP Race)

```sql
SELECT 
    driver,
    team,
    avg_lap_time_s,
    best_lap_rank,
    points
FROM gold_driver_performance
WHERE year = 2025 AND round = 1 AND session_type = 'R'
ORDER BY points DESC;
```

### Query: Team Season Trends (2024)

```sql
SELECT 
    team,
    total_points,
    avg_pace,
    driver_count
FROM gold_team_season_stats
WHERE year = 2024
ORDER BY total_points DESC;
```

### Query: Lap Time Distribution (Qualifying vs Race)

```sql
SELECT 
    session_type,
    AVG(lap_time_s) as avg_lap_time,
    MIN(lap_time_s) as fastest,
    MAX(lap_time_s) as slowest
FROM silver_laps
WHERE year = 2025 AND round = 1
GROUP BY session_type;
```

---

## 📚 Additional Resources

- **FastF1 Documentation:** https://docs.fastf1.dev
- **Apache Spark SQL:** https://spark.apache.org/docs/latest/sql-guide.html
- **PostgreSQL Docs:** https://www.postgresql.org/docs/
- **Architecture Deep Dive:** [diagramy/pipeline_architecture.md](diagramy/pipeline_architecture.md)

---

## 🛠️ Development & Contributing

### Running Tests

```bash
# Smoke test (recommended first step)
python pipeline.py --smoke --smoke-year 2025 --smoke-round 1 --smoke-session R

# Single year (faster full test)
python pipeline.py --smoke-year 2024
```

### Code Structure

- `src/orchestrator.py` — Stage coordination (don't modify unless adding new stages)
- `src/silver.py` — Transform logic (safe to extend)
- `src/config.py` — Environment handling

---

## 📄 License

This project is provided as-is for educational and analytical purposes.
