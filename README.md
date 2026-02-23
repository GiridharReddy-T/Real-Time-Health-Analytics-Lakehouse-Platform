# 🏥 Real-Time-Health-Analytics-Lakehouse-Platform
Built an end-to-end real-time data engineering platform on Databricks Lakehouse using Medallion Architecture (Bronze→Silver→Gold) for a wearable health device company. Ingested IoT health metrics via Kafka &amp; Auto Loader, applied CDC merges, stream-stream joins, and delivered BPM &amp; gym analytics using Delta Lake.

### Databricks | Apache Kafka | Delta Lake | Medallion Architecture | Azure DevOps CI/CD

> An end-to-end real-time data engineering platform built for a wearable health device company —
> ingesting continuous IoT health metrics from multiple Kafka streams and batch sources, processing
> through a Medallion Lakehouse architecture, and delivering analytical-ready datasets for health
> and fitness center reporting.

---

## 📌 Project Overview

This project simulates a production-grade data engineering system for a **wearable device manufacturing company**. End users wear a health-monitoring wristband that continuously transmits health parameters (heart rate, workout sessions, gym activity) to company servers. The platform collects, processes, and delivers two key gold-layer analytical outputs:

- **Workout BPM Summary** — per-user workout heart rate analytics (min / avg / max BPM)
- **Gym Summary** — per-fitness-center user visit and active exercise time reporting

The system is built on **Databricks Lakehouse** with **Delta Lake**, supports both **batch and streaming** workflows, and includes a fully automated **CI/CD pipeline** using Azure DevOps.

---

## 📊 Data Sources & Datasets

| # | Dataset | Format | Transport | Description |
|---|---------|--------|-----------|-------------|
| 1 | Device Registration | CSV | Cloud Storage (Landing Zone) | User ID, Device ID, MAC Address, Registration Timestamp |
| 2 | User Profile CDC Events | JSON | Kafka → Multiplex Topic | Insert / Update / Delete profile changes from mobile app |
| 3 | BPM Heart Rate Stream | JSON | Kafka → Multiplex Topic | Continuous IoT heartrate events from wearable device |
| 4 | Workout Session Events | JSON | Kafka → Multiplex Topic | Start / Stop workout button press events |
| 5 | Gym Login / Logout Events | CSV | Cloud Storage (Landing Zone) | Facility scanner entry and exit events |

> **Design Note:** Kafka topics 2, 3, and 4 are ingested via a single **Kafka Multiplex** bronze table, partitioned by `topic` and `week_part` for efficient downstream filtering.

---

## 🏗️ Architecture

```
SOURCE SYSTEMS
─────────────────────────────────────────────────────────────────
  Cloud Storage    →   Device Registration CSV Files
  Cloud Storage    →   Gym Login/Logout CSV Files
  Kafka Topics     →   User Profile CDC + BPM Stream + Workout Events
                       (Multiplexed into single landing zone)

MEDALLION LAKEHOUSE  (Databricks + Delta Lake + Unity Catalog)
─────────────────────────────────────────────────────────────────

  ┌─────────────────────────────────────────────────────┐
  │                   BRONZE LAYER                      │
  │  Raw ingestion using Spark Structured Streaming     │
  │  (Auto Loader / cloudFiles format)                  │
  │                                                     │
  │  registered_users_bz   → CSV batch files            │
  │  gym_logins_bz         → CSV batch files            │
  │  kafka_multiplex_bz    → JSON Kafka events          │
  │  (Partitioned by topic + week_part)                 │
  │  Audit: load_time, source_file columns added        │
  └───────────────────────┬─────────────────────────────┘
                          │
                          ▼
  ┌─────────────────────────────────────────────────────┐
  │                   SILVER LAYER                      │
  │  Cleaned, Deduplicated, CDC Merged tables           │
  │                                                     │
  │  Phase 1:                                           │
  │    users             ← dedup + MERGE (insert-only)  │
  │    gym_logs          ← MERGE (insert + update)      │
  │    user_profile      ← CDC MERGE (SCD Type 1)       │
  │    workouts          ← dedup + MERGE                │
  │    heart_rate        ← dedup + MERGE + valid flag   │
  │                                                     │
  │  Phase 2:                                           │
  │    user_bins         ← age binning + MERGE          │
  │    completed_workouts← stream-stream join           │
  │                        (start + stop events)        │
  │                                                     │
  │  Phase 3:                                           │
  │    workout_bpm       ← stream-stream join           │
  │                        (BPM × completed_workouts)   │
  └───────────────────────┬─────────────────────────────┘
                          │
                          ▼
  ┌─────────────────────────────────────────────────────┐
  │                    GOLD LAYER                       │
  │  Analytical-ready aggregated outputs                │
  │                                                     │
  │  workout_bpm_summary → Streaming aggregation        │
  │                         min/avg/max BPM per session │
  │  gym_summary         → SQL View                     │
  │                         visit duration + exercise   │
  └─────────────────────────────────────────────────────┘

DATA CONSUMERS
─────────────────────────────────────────────────────────────────
  BI Dashboards | Health Reports | Fitness Center Analytics
```

---

## 🥇 Gold Layer Outputs

### 1. Workout BPM Summary (`workout_bpm_summary`)
Per-user, per-session heart rate analytics enriched with user demographic bins.

| Column | Type | Description |
|--------|------|-------------|
| workout_id | INT | Workout identifier |
| session_id | INT | Session identifier |
| user_id | BIGINT | Unique user |
| age | STRING | Age bin (e.g. "25-35", "35-45") |
| gender | STRING | User gender |
| city | STRING | User city |
| state | STRING | User state |
| min_bpm | DOUBLE | Minimum heart rate during session |
| avg_bpm | DOUBLE | Average heart rate during session |
| max_bpm | DOUBLE | Maximum heart rate during session |
| num_recordings | BIGINT | Total BPM readings captured |

### 2. Gym Summary (`gym_summary`)
SQL View combining gym login/logout with completed workout sessions for fitness center reporting.

| Column | Description |
|--------|-------------|
| date | Visit date |
| gym | Gym / facility identifier |
| mac_address | Device MAC (user identifier) |
| workout_id / session_id | Linked workout session |
| minutes_in_gym | Total time inside facility |
| minutes_exercising | Active workout duration |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| Data Processing | Apache Spark, PySpark |
| Streaming Engine | Spark Structured Streaming |
| Storage Format | Delta Lake |
| Ingestion | Auto Loader (cloudFiles) |
| Platform | Databricks Lakehouse |
| Data Governance | Unity Catalog (multi-env catalog) |
| State Store | RocksDB State Store Provider |
| Orchestration | Databricks Workflows |
| CI/CD | Azure DevOps Build Pipeline |
| Languages | Python, SQL |
| Environments | DEV → TEST → PROD |

---

## 📁 Project Structure

```
Capstone Project/
│
├── Notebooks/
│   ├── 01-config.py            # Config class: external locations, DB name, tuning params
│   ├── 02-setup.py             # SetupHelper: create/validate/cleanup all Delta tables
│   ├── 03-history-loader.py    # HistoryLoader: seed date_lookup reference table
│   ├── 04-bronze.py            # Bronze class: Auto Loader streaming ingestion
│   ├── 05-silver.py            # Silver class: CDC merge, dedup, stream-stream joins
│   ├── 06-gold.py              # Gold class: aggregations + gym_summary view
│   ├── 07-run.py               # Main entrypoint: orchestrates full pipeline run
│   ├── 08-batch-test.py        # Integration test: batch mode end-to-end validation
│   ├── 09-stream-test.py       # Integration test: streaming mode validation
│   └── 10-producer.py          # Producer: simulates data arrival in landing zone
│
├── Data Set/
│   ├── 1-registered_users_*.csv       # Device registration data (2 sets)
│   ├── 2-user_info_*.json             # User profile CDC events (2 sets)
│   ├── 3-bpm_*.json                   # Heart rate BPM stream (2 sets, 253K records each)
│   ├── 4-workout_*.json               # Workout start/stop events (2 sets)
│   ├── 5-gym_logins_*.csv             # Gym login/logout events (2 sets)
│   ├── 6-date-lookup.json             # Date dimension reference table (365 records)
│   ├── 7-gym_summary_*.parquet        # Expected gold output for validation
│   └── 8-workout_bpm_summary_*.parquet # Expected gold output for validation
│
└── Other Code/
    ├── azure_build_pipeline.yaml      # Azure DevOps CI build pipeline
    ├── deploy.sh                      # Deployment shell script
    ├── deploy-notebooks.sh            # Notebook deployment script
    ├── run-integration-test.sh        # Integration test runner script
    ├── SBIT Deploy Pipeline.json      # Databricks job deploy config
    └── DemoJobDeploy/Deploy.py        # Python deployment helper
```

---

## 🔄 Data Flow & Processing Logic

### Bronze Layer — Raw Ingestion
- Uses **Auto Loader** (`cloudFiles` format) for incremental file detection
- Adds `load_time` (audit timestamp) and `source_file` (lineage) to every record
- `kafka_multiplex_bz` is **partitioned by `topic` and `week_part`** for efficient downstream reads
- Date enrichment via **broadcast join** with `date_lookup` table on ingestion

### Silver Layer — Phased Upserts (3 Phases)

**Phase 1 — Core tables (parallel streams):**
- `users` — Deduplication on `(user_id, device_id)` + insert-only MERGE
- `gym_logs` — MERGE with conditional logout update logic
- `user_profile` — **CDC MERGE** handling `new`, `update`, `delete` event types with SCD Type 1 logic; latest record per user_id selected via window ranking
- `workouts` — Dedup on `(user_id, time)` + insert-only MERGE
- `heart_rate` — Dedup + BPM validity flag (`valid = heartrate > 0`)

**Phase 2 — Derived tables:**
- `user_bins` — Stream-to-static join with age binning logic (10 age buckets); SCD Type 1 MERGE
- `completed_workouts` — **Stream-stream join** matching `start` and `stop` workout events with 3-hour state timeout

**Phase 3 — Enriched join:**
- `workout_bpm` — **Stream-stream join** matching BPM readings within `(start_time, end_time)` of completed workouts; 3-hour watermark for state cleanup

### Gold Layer — Aggregation
- `workout_bpm_summary` — Streaming group-by aggregation on `(user_id, workout_id, session_id)` computing min/avg/max BPM, joined with `user_bins` for demographic enrichment
- `gym_summary` — SQL View joining `gym_logs` with `completed_workouts` + `users` to calculate time in gym vs. time exercising

---

## ⚙️ Key Engineering Highlights

**Idempotent MERGE Operations**
All silver and gold writes use Delta Lake `MERGE` statements ensuring exactly-once semantics even on pipeline reruns.

**CDC Handling with Window Ranking**
User profile CDC events (insert/update/delete) are processed using Spark window functions to select the latest record per user before merging — preventing stale updates from out-of-order events.

**Stream-Stream Joins with Watermarks**
Completed workouts are derived by joining two independent streams (`start` and `stop` events) with a **3-hour watermark** for automatic state cleanup. The same pattern is used to match BPM readings to their workout window.

**Kafka Multiplex Pattern**
Instead of separate bronze tables per Kafka topic, all Kafka data lands in a single multiplexed table partitioned by `topic` — reducing ingestion overhead while allowing efficient topic-based filtering in silver.

**Spark Scheduler Pools**
Different streams are assigned to named scheduler pools (`bronze_p1`, `silver_p1`, etc.) to control resource allocation and prioritization across concurrent streams.

**RocksDB State Store**
Streaming state is managed using the **RocksDB State Store Provider** for improved performance with large stateful operations like stream-stream joins.

**Multi-Environment Support**
The Unity Catalog is used as the environment namespace (`dev`, `test`, `prod`), allowing the same codebase to run across all environments without code changes — controlled via Databricks widget parameters.

---

## 🧪 Automated Integration Testing

The `08-batch-test.py` notebook implements a full end-to-end integration test:

1. Cleans up the environment (drops DB, deletes landing zone)
2. Runs full setup and history load
3. Produces **Set 1** of test data via `Producer` class
4. Runs the pipeline in batch mode
5. Validates record counts at Bronze, Silver, and Gold layers
6. Produces **Set 2** (incremental load)
7. Re-runs pipeline and validates again with cumulative expected counts
8. Compares Gold layer output against expected Parquet files row-by-row

**Sample validation counts (Set 1 / Set 2):**

| Table | Set 1 | Set 2 |
|-------|-------|-------|
| registered_users_bz | 5 | 10 |
| gym_logins_bz | 8 | 16 |
| kafka_multiplex_bz (bpm) | 253,801 | 507,602 |
| users | 5 | 10 |
| heart_rate | 253,801 | 507,602 |
| completed_workouts | 8 | 16 |
| workout_bpm | 3,968 | 8,192 |

---

## 🚀 CI/CD Pipeline (Azure DevOps)

The project includes a fully automated **Azure DevOps build pipeline** (`azure_build_pipeline.yaml`):

- Triggers on every branch push
- Sets up Python 3.10 environment
- Installs dependencies (pytest, requests)
- Packages notebooks into a build artifact (ZIP)
- Publishes artifact as `DatabricksBuild`

Deployment scripts (`deploy.sh`, `deploy-notebooks.sh`) handle notebook promotion to Databricks workspaces across environments. Integration tests are triggered via `run-integration-test.sh`.

---

## 🚀 Getting Started

### Prerequisites
- Databricks Workspace with **Unity Catalog** enabled
- Two External Locations configured: `data_zone` and `checkpoint`
- Python 3.8+
- Azure DevOps (for CI/CD)

### Run the Pipeline

```python
# In Databricks, run 07-run.py with parameters:
# Environment = "dev"       (or "test" / "prod")
# RunType     = "once"      (batch) or "continuous" (streaming)
# ProcessingTime = "5 seconds"  (for streaming mode)
```

### Run Integration Tests

```bash
# Trigger from CI or manually
bash run-integration-test.sh
```

---

## 📈 Business Value

- Enables **real-time health monitoring** for end users through continuous BPM tracking with validity checks
- Provides **fitness centers** with accurate visit duration and active exercise analytics
- Delivers **governed, auditable data** through Unity Catalog with lineage via source_file tracking
- Supports **incremental processing** — pipeline handles new data batches without full reprocessing
- **Multi-environment architecture** ensures safe promotion from DEV → TEST → PROD

---

## 🙋 Author

**Giridhar Reddy T**
Data Engineer | Databricks | Spark | Delta Lake | Kafka
[LinkedIn](https://www.linkedin.com/in/giridhar-reddy-tatiparthi-272b94244/) • [GitHub](https://github.com/GiridharReddy-T)

---

## 📄 License
MIT License
