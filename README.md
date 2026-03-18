# snowflake-to-bigquery-migration-toolkit

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![GCP](https://img.shields.io/badge/GCP-BigQuery-orange)
![License](https://img.shields.io/badge/license-MIT-green)

Production toolkit for migrating Snowflake data warehouses to BigQuery with automated schema translation, data validation, and zero-downtime cutover.

## Features

- **Automated Schema Translation** - Maps all Snowflake data types to BigQuery equivalents (NUMBER, VARIANT, TIMESTAMP_NTZ, etc.)
- **Data Validation** - Row count reconciliation, MD5 checksums, and per-column null-count comparison
- **Partitioning & Clustering** - Auto-applies BQ partitioning and clustering from Snowflake metadata
- **Zero-Downtime Cutover** - Dual-write strategy with incremental sync and validation gate
- **Markdown Reports** - Automated validation summary reports per migration wave

## Architecture

```
Snowflake (Source)
    │
    ├── schema_translator.py   # DDL type mapping & BQ table creation
    ├── data_validator.py      # Row count, checksum & null validation
    └── cutover_manager.py     # Dual-write orchestration
         │
         └── BigQuery (Target)
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Set environment variables
export SNOWFLAKE_ACCOUNT=myaccount
export SNOWFLAKE_USER=myuser
export SNOWFLAKE_PASSWORD=mypassword
export GCP_PROJECT_ID=my-gcp-project
export BQ_DATASET=migrated_dataset

# Translate and create schema
python src/schema_translator.py

# Validate migrated data
python src/data_validator.py
```

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

## Validation Output

```
| Table       | SF Rows    | BQ Rows    | Rows Match | Checksum Match | Status  |
|-------------|------------|------------|------------|----------------|---------|
| users       | 10,234,891 | 10,234,891 | True       | True           | PASS    |
| orders      | 45,678,231 | 45,678,231 | True       | True           | PASS    |
| sessions    | 203,441,002| 203,441,002| True       | True           | PASS    |
```

## Project Structure

```
snowflake-to-bigquery-migration-toolkit/
├── src/
│   ├── schema_translator.py   # Core type mapping & table creation
│   └── data_validator.py      # Validation framework
├── tests/
│   └── test_schema_translator.py
├── requirements.txt
└── README.md
```

## Tech Stack

- **Python 3.10+**
- **Google Cloud BigQuery** (`google-cloud-bigquery`)
- **Snowflake Connector** (`snowflake-connector-python`)
- **PyArrow** for efficient data serialization

## License

MIT
