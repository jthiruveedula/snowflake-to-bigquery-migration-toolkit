# snowflake-to-bigquery-migration-toolkit

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![GCP](https://img.shields.io/badge/GCP-BigQuery-orange)
![License](https://img.shields.io/badge/license-MIT-green)

Production toolkit for migrating Snowflake data warehouses to BigQuery, covering the full lifecycle:
assessment, SQL translation, schema creation, bulk + incremental transfer, validation, and zero-downtime
cutover. Built on Google-managed services (BigQuery Migration Service, BigQuery Data Transfer Service)
wherever possible.

📖 **[Full documentation site](./index.html)** — module reference, lifecycle diagram, usage examples.

## Features

- **Assessment & Discovery** - Inventories Snowflake objects and builds a dependency/size-aware migration wave plan
- **SQL Translation** - Translates views, stored procedures, UDFs, and queries via the BigQuery Migration Service SQL translation API
- **Automated Schema Translation** - Maps Snowflake data types to BigQuery equivalents, with clustering/partition auto-detection
- **Bulk & Incremental Transfer** - GCS-staged bulk loads plus BigQuery Data Transfer Service for recurring syncs
- **Incremental Sync** - Watermark/CDC-based MERGE, with full-reload fallback when no watermark exists
- **Data Validation** - Row count, checksum (full or sampled for large tables), null-count, and column-type comparisons
- **Zero-Downtime Cutover** - Dual-write orchestration with a validation gate and rollback support
- **Single CLI** - `python -m src.cli <command>` ties every phase together, with per-wave state persistence

## Architecture

```
Snowflake (Source)
    │
    ├── assessment.py          # Inventory + migration wave plan
    ├── sql_translator.py      # BQ Migration Service SQL translation
    ├── schema_translator.py   # DDL type mapping & BQ table creation
    ├── data_transfer.py       # GCS-staged bulk load + BQ DTS config
    ├── incremental_sync.py    # Watermark/stream-based MERGE sync
    ├── data_validator.py      # Row count, checksum & null validation
    ├── cutover_manager.py     # Dual-write orchestration + rollback
    └── migrator.py / cli.py   # Config-driven, wave-based orchestration
         │
         └── BigQuery (Target)
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables (read by config/migration_config.yaml)
export SNOWFLAKE_ACCOUNT=myaccount
export SNOWFLAKE_USER=myuser
export SNOWFLAKE_PASSWORD=mypassword
export GCP_PROJECT_ID=my-gcp-project
export GCS_STAGING_BUCKET=my-migration-bucket

# Edit config/migration_config.yaml with your tables and wave assignments

# Run the full lifecycle
python -m src.cli assess
python -m src.cli translate-sql
python -m src.cli create-schema
python -m src.cli transfer
python -m src.cli sync
python -m src.cli validate
python -m src.cli cutover
```

Or via `make`:

```bash
make install
make assess transfer validate cutover
```

## Migration Lifecycle

| Phase | Command | Module |
|-------|---------|--------|
| 1. Assess | `assess` | `assessment.py` |
| 2. Translate SQL | `translate-sql` | `sql_translator.py` |
| 3. Create Schema | `create-schema` | `schema_translator.py` |
| 4. Transfer Data | `transfer` | `data_transfer.py` |
| 5. Incremental Sync | `sync` | `incremental_sync.py` |
| 6. Validate | `validate` | `data_validator.py` |
| 7. Cutover | `cutover` | `cutover_manager.py` |

Each phase persists completion state to `migration_state.json`, so any command is safe to re-run without
redoing already-completed tables.

## Data Type Mapping

| Snowflake Type | BigQuery Type |
|----------------|---------------|
| NUMBER(p,0) | INT64 |
| NUMBER(p,s) | NUMERIC |
| VARCHAR / TEXT | STRING |
| TIMESTAMP_NTZ | DATETIME |
| TIMESTAMP_LTZ | TIMESTAMP |
| VARIANT / OBJECT | JSON |
| BOOLEAN | BOOL |
| GEOGRAPHY | GEOGRAPHY |

## Configuration

All connection details live in `config/migration_config.yaml`, with secrets resolved from environment
variables — never hardcoded:

```yaml
snowflake:
  account_env: SNOWFLAKE_ACCOUNT
  user_env: SNOWFLAKE_USER
  password_env: SNOWFLAKE_PASSWORD
  database: ANALYTICS
bigquery:
  project_id_env: GCP_PROJECT_ID
  dataset: migrated_dataset
tables:
  - sf_table: analytics.public.users
    bq_table: users
    key_col: user_id
    watermark_col: updated_at
    wave: 1
```

## Testing

```bash
pytest tests/ -v
# or
make test
```

All Snowflake/BigQuery/GCS/DTS clients are mocked — no live credentials required to run the suite.
CI runs the suite on every push and pull request via `.github/workflows/tests.yml`.

## Documentation Site

`index.html` is a self-contained, single-page documentation site covering every module, deployed to
GitHub Pages via `.github/workflows/pages.yml` on every push to `main`.

## Project Structure

```
snowflake-to-bigquery-migration-toolkit/
├── src/
│   ├── assessment.py
│   ├── sql_translator.py
│   ├── schema_translator.py
│   ├── data_transfer.py
│   ├── incremental_sync.py
│   ├── data_validator.py
│   ├── cutover_manager.py
│   ├── migrator.py
│   └── cli.py
├── config/
│   └── migration_config.yaml
├── tests/
│   └── test_*.py
├── .github/workflows/
│   ├── tests.yml
│   └── pages.yml
├── index.html
├── Makefile
├── pyproject.toml
├── requirements.txt
└── README.md
```

## Tech Stack

- **Python 3.10+**
- **Google Cloud BigQuery**, **BigQuery Migration Service**, **BigQuery Data Transfer Service**, **Cloud Storage**
- **Snowflake Connector** (`snowflake-connector-python`)
- **Click** for the CLI, **PyYAML** for config, **structlog** for logging

## License

MIT
