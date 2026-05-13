<img src="https://d1r5llqwmkrl74.cloudfront.net/notebooks/fsi/fs-lakehouse-logo-transparent.png" width="600px">
# Data Migration Accelerator
 
[![Databricks](https://img.shields.io/badge/Databricks-Solution%20Accelerator-FF3621)](https://marketplace.databricks.com)
[![DBR](https://img.shields.io/badge/DBR-14.3%20LTS%2B-blue)](https://docs.databricks.com/release-notes/runtime/14.3.html)
[![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-Supported-green)](https://docs.databricks.com/data-governance/unity-catalog/index.html)
[![Python](https://img.shields.io/badge/Python-3.10%2B-yellow)](https://www.python.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue)](https://opensource.org/licenses/Apache-2.0)
 
---
 
## Overview
 
The **Teradata → Databricks Data Migration Accelerator** is a production-grade, fully automated pipeline that moves data from **Teradata** into **Databricks Delta Lake** via **Apache Parquet** and **Amazon S3**. Engineered for enterprise scale, the accelerator handles everything from schema discovery to data ingestion and cross-system reconciliation — with real-time email alerting at every step.
 
This solution dramatically reduces the time, risk, and manual effort of migrating large, complex Teradata workloads to the Databricks Lakehouse. With a single configuration change, teams can switch between cloud and on-premises Teradata deployments, and the architecture is designed to accommodate **SAP** and **Hadoop** as future source systems without rewriting any orchestration logic.
 
| What you get | Details |
|---|---|
| **Zero-code migration** | Configure INI files — the pipeline handles the rest |
| **Automatic schema discovery** | Column definitions read live from Teradata and stored as a versioned registry |
| **Parquet-based staging** | All data lands in Parquet on S3 before being loaded into Delta Lake |
| **Row-level reconciliation** | Source vs target row counts, column counts, and checksums verified automatically |
| **Per-table email alerts** | Success or failure email sent for every table after every run |
| **Audit trail** | Every run recorded in `pipeline_control.csv` — stored locally and on S3 |
| **Modular & extensible** | Swap source connectors without touching orchestration logic |
 
---
 
## Architecture
 
```
Teradata                    Accelerator Modules                    Databricks Delta Lake
────────────    ──────────────────────────────────────────────    ─────────────────────
Source Tables ──► 1. PREFLIGHT      (connectivity checks)
                  2. SCHEMA SYNC    (capture column definitions) ──► schema_registry.json
                  3. EXTRACT        (read tables → Parquet)      ──► local Parquet files
                  4. S3 UPLOAD      (Parquet → S3 bucket)        ──► S3 staging area
                  5. DDL            (CREATE Delta tables)        ──► Unity Catalog tables
                  6. INGEST         (TRUNCATE + COPY INTO)       ──► Delta Lake rows
                  7. RECONCILIATION (row & column comparison)    ◄── verify counts match
                  8. ALERTS         (Mailgun email per table)
                  9. CONTROL FILE   (audit trail → S3)           ──► pipeline_control.csv
```
 
---
 
## Prerequisites
 
Before installation, ensure the following are available in your environment:
 
| Requirement | Details |
|---|---|
| **Python** | 3.10 or higher |
| **Databricks Runtime** | 14.3 LTS or above |
| **Unity Catalog** | Recommended (Hive Metastore also supported) |
| **AWS S3** | An existing S3 bucket — the pipeline does not create it |
| **Mailgun** | API key and verified sending domain for email alerts |
| **Network** | Connectivity from execution environment to the Teradata host and Databricks SQL Warehouse |
 
---

## Installation & Setup
 
### Step 1 — Configure INI Files
 
Open the `datapipeline.dist/config/` folder and fill in your credentials and settings in each `.ini` file. Also configure the tables you want to migrate in `datapipeline.dist/metadata/pipeline_jobs.ini`.
 
```
datapipeline.dist/
├── config/
│   ├── alerts.ini           ← Mailgun API key, from/to addresses
│   ├── databricks.ini       ← Workspace URL, warehouse path, catalog, schema
│   ├── s3.ini               ← S3 bucket, region, credentials
│   ├── sources.ini          ← Teradata connection details & active source
│   └── paths.ini            ← Output paths for logs, artifacts, control file
└── metadata/
    ├── pipeline_jobs.ini    ← Tables, LOAD_TYPE, WATERMARK_COLUMNS, PRIMARY_KEYS
    └── type_mapping.ini     ← Teradata → Databricks data type mappings
```
 
Refer to the [Configuration Reference](#configuration-reference) section below for details on each setting.
 
> **Security note:** Never store passwords or tokens directly in INI files. Use environment variables instead — the pipeline reads credentials from `TERADATA_PASSWORD`, `DATABRICKS_TOKEN`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` automatically.
 
---
 
### Step 2 — Run the Application
 
```bash
.\datapipeline.dist\datapipeline.exe
```
 
#### Optional flags
 
```bash
# Force a specific table to do a full reload this run
.\datapipeline.dist\datapipeline.exe --force-full orders
 
# Force multiple tables to full reload
.\datapipeline.dist\datapipeline.exe --force-full orders --force-full customers
 
# Verify connectivity only — no data is moved
.\datapipeline.dist\datapipeline.exe --test-connection
 
# Run preflight checks only
.\datapipeline.dist\datapipeline.exe --preflight-only
```
 
---
 
## Configuration Reference
 
### `config/sources.ini`
 
Defines the Teradata connection details and which source/deployment to use.
 
```ini
[sources]
active_source = teradata
active_deployment = cloud
 
[SOURCE.TERADATA.CLOUD]
host = your-teradata-host.company.com
username = your_username
# Do not store passwords in files. Use env var TERADATA_PASSWORD instead.
password =
```
 
---
 
### `config/databricks.ini`
 
Defines your Databricks workspace connection and target schema.
 
```ini
[databricks]
host           = https://your-workspace.cloud.databricks.com
http_path      = /sql/1.0/warehouses/your_warehouse_id
# Leave empty to use environment variable DATABRICKS_TOKEN
access_token   =
catalog        = main
target_schema  = migration_target
ddl_enabled    = true
ingest_enabled = true
```
 
---
 
### `config/s3.ini`
 
Defines the S3 bucket used for Parquet staging and audit file storage.
 
```ini
[s3]
enabled               = true
bucket                = your-migration-bucket
prefix                = teradata/extract
region                = us-east-1
# Prefer IAM role / default credential chain; if needed use env vars:
# AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
aws_access_key_id     =
aws_secret_access_key =
control_prefix        = teradata/control
```
 
---
 
### `config/alerts.ini`
 
Configures Mailgun email alerts.
 
```ini
[mailgun]
api_key        = your-mailgun-api-key
sending_domain = mg.yourcompany.com
from_address   = pipeline@mg.yourcompany.com
to_address     = your-team@yourcompany.com
```
 
---
 
### `metadata/pipeline_jobs.ini`
 
Lists the tables to migrate and the Teradata source schema.
 
```ini
[SCHEMA]
source_db = YOUR_TERADATA_DATABASE
 
[TABLENAMES]
customers
orders
products
```
 
---
 
## Permissions Requirements
 
The service account or user running the pipeline needs the following permissions:
 
**Teradata:**
- `SELECT` on all source tables and schemas
- `SELECT` on `DBC.*` system tables (for schema discovery)
**AWS S3:**
- `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` on the configured bucket
**Databricks:**
- `CAN USE` on the target SQL Warehouse
- `CREATE TABLE`, `MODIFY` on the target schema in Unity Catalog
- A valid Personal Access Token (PAT) or OAuth credential
**Mailgun:**
- A verified sending domain and API key with `send` permissions
---
 
## Pipeline Flow
 
```
  run-pipeline
       │
       ▼
  ┌─────────────────────┐
  │   Load Configuration │
  └──────────┬──────────┘
             │
             ▼
       Config valid?
       ├── No  ──► EXIT 2: Config Error
       └── Yes
             │
             ▼
  ┌──────────────────────┐
  │  Run Preflight Checks │
  └──────────┬───────────┘
             │
             ▼
       Preflight OK?
       ├── No  ──► EXIT 3: Preflight Failed
       └── Yes
             │
             ▼
  ┌────────────────────────────────────┐
  │  Download pipeline_control.csv     │
  │  from S3                           │
  └──────────┬─────────────────────────┘
             │
             ▼
  ┌──────────────────────┐
  │  Connect to Teradata  │
  └──────────┬───────────┘
             │
             ▼
       Connected?
       ├── No  ──► EXIT 4: Connection Failed
       └── Yes
             │
             ▼
  ┌──────────────────────────────────────┐
  │  Schema Sync                          │
  │  Read column definitions from Teradata│
  │  → schema_registry.json              │
  └──────────┬───────────────────────────┘
             │
             ▼
  ┌──────────────────────────────────────────────────────────────┐
  │              PER-TABLE PIPELINE LOOP                          │
  │  (each table is isolated — one failure never blocks others)  │
  │                                                              │
  │   ┌────────────────────────────────────────────────────┐     │
  │   │  EXTRACT: SELECT * FROM table → local Parquet file │     │
  │   └───────────────────┬────────────────────────────────┘     │
  │                       ▼                                      │
  │   ┌────────────────────────────────────────────────────┐     │
  │   │  S3 UPLOAD: Parquet → S3 bucket/prefix             │     │
  │   └───────────────────┬────────────────────────────────┘     │
  │                       ▼                                      │
  │   ┌────────────────────────────────────────────────────┐     │
  │   │  DDL: CREATE TABLE IF NOT EXISTS in Databricks     │     │
  │   └───────────────────┬────────────────────────────────┘     │
  │                       ▼                                      │
  │   ┌────────────────────────────────────────────────────┐     │
  │   │  INGEST: TRUNCATE TABLE + COPY INTO from S3        │     │
  │   └───────────────────┬────────────────────────────────┘     │
  │                       ▼                                      │
  │   ┌────────────────────────────────────────────────────┐     │
  │   │  RECONCILIATION: Source row count vs Target        │     │
  │   │  Column counts · Numeric aggregates                │     │
  │   └───────────────────┬────────────────────────────────┘     │
  │                       ▼                                      │
  │           Recon PASS? ──► SUCCESS · Send Alert Email         │
  │           Recon FAIL? ──► FAILED  · Send Alert Email         │
  │                                                              │
  │   ◄─────────── Repeat for next table ─────────────────────   │
  └──────────────────────────────────────────────────────────────┘
             │
             ▼
  ┌────────────────────────────────────┐
  │  Upload pipeline_control.csv to S3 │
  └──────────┬─────────────────────────┘
             │
             ▼
       Overall result?
       ├── All SUCCESS  ──► EXIT 0 ✓
       ├── Some FAILED  ──► EXIT 8  (partial)
       └── All FAILED   ──► EXIT 5
```
 
---
 
## Exit Codes
 
| Code | Meaning |
|---|---|
| `0` | All tables migrated and reconciled successfully |
| `2` | Configuration error — check your INI files |
| `3` | Preflight failed — connectivity or path issue |
| `4` | Teradata connection failed |
| `5` | All tables failed |
| `8` | Partial success — some tables failed, some succeeded |
 
---
 
## Artifacts & Outputs
 
After each run, the following outputs are generated automatically:
 
```
artifacts/
├── pipeline_control.csv       ← Full audit trail (also synced to S3)
├── logs/                      ← One log file per run
└── output/
    ├── extract/               ← Parquet files per table per run
    ├── recon/                 ← Reconciliation JSON files per table per run
    └── schema/                ← schema_registry.json
```
 
### Audit Trail — `pipeline_control.csv`
 
Every table run produces one row with full metadata:
 
| Column | Description |
|---|---|
| `RUN_ID` | Unique auto-incrementing identifier per table run |
| `SOURCE_SCHEMA_NAME` | Teradata source schema/database |
| `SOURCE_OBJECT_NAME` | Source table name |
| `TARGET_DATABASE_NAME` | Databricks catalog |
| `TARGET_SCHEMA_NAME` | Databricks schema |
| `TARGET_TABLE_NAME` | Databricks table name |
| `STATUS` | `SUCCESS`, `FAILED`, or `STARTED` |
| `RUN_START` / `RUN_END` | UTC timestamps |
| `DURATION_IN_SEC` | Wall-clock duration |
| `SOURCE_ROW_COUNT` | Rows read from Teradata |
| `TARGET_ROW_COUNT` | Rows in Databricks after load |
| `RECON_COMPARE_STATUS` | `PASS` or `FAIL` |
| `ERROR_MESSAGE` | Failure reason if applicable |
 
### Reconciliation Output Sample
 
```json
{
  "run_id": 12,
  "table": "customers",
  "status": "PASS",
  "source_row_count": 150000,
  "target_row_count": 150000,
  "source_col_count": 24,
  "target_col_count": 24,
  "columns": {
    "customer_id": { "source_distinct": 150000, "target_distinct": 150000, "match": true },
    "revenue":     { "source_sum": 9842310.50,  "target_sum": 9842310.50,  "match": true }
  }
}
```
 
---
 
## Library Dependencies
 
| Library | Purpose | License |
|---|---|---|
| `teradatasql` | Teradata Python connector | Teradata |
| `pandas` | DataFrame operations and Parquet I/O | BSD |
| `pyarrow` | Parquet file format support | Apache 2.0 |
| `boto3` | AWS S3 client | Apache 2.0 |
| `databricks-sql-connector` | Databricks SQL connection | Apache 2.0 |
 
---
 
Licensed under the [Apache License, Version 2.0](https://opensource.org/licenses/Apache-2.0). All included or referenced third-party libraries are subject to the licenses listed above.
