"""Snowflake to BigQuery Schema Translator

Translates Snowflake DDL schemas to BigQuery-compatible schemas,
handling data type mappings, clustering keys, and partitioning.
"""

import re
import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Snowflake -> BigQuery type mapping
TYPE_MAPPING: Dict[str, str] = {
    "NUMBER": "NUMERIC",
    "DECIMAL": "NUMERIC",
    "NUMERIC": "NUMERIC",
    "INT": "INT64",
    "INTEGER": "INT64",
    "BIGINT": "INT64",
    "SMALLINT": "INT64",
    "TINYINT": "INT64",
    "BYTEINT": "INT64",
    "FLOAT": "FLOAT64",
    "FLOAT4": "FLOAT64",
    "FLOAT8": "FLOAT64",
    "DOUBLE": "FLOAT64",
    "REAL": "FLOAT64",
    "VARCHAR": "STRING",
    "CHAR": "STRING",
    "CHARACTER": "STRING",
    "STRING": "STRING",
    "TEXT": "STRING",
    "BINARY": "BYTES",
    "VARBINARY": "BYTES",
    "BOOLEAN": "BOOL",
    "DATE": "DATE",
    "DATETIME": "DATETIME",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP_NTZ": "DATETIME",
    "TIMESTAMP_LTZ": "TIMESTAMP",
    "TIMESTAMP_TZ": "TIMESTAMP",
    "VARIANT": "JSON",
    "OBJECT": "JSON",
    "ARRAY": "JSON",
    "GEOGRAPHY": "GEOGRAPHY",
}


@dataclass
class ColumnDefinition:
    name: str
    sf_type: str
    bq_type: str
    nullable: bool = True
    default: Optional[str] = None
    comment: Optional[str] = None
    precision: Optional[int] = None
    scale: Optional[int] = None


@dataclass
class TableSchema:
    database: str
    schema: str
    table_name: str
    columns: List[ColumnDefinition] = field(default_factory=list)
    cluster_keys: List[str] = field(default_factory=list)
    partition_col: Optional[str] = None
    row_count: int = 0


class SnowflakeToBigQueryTranslator:
    """Translates Snowflake table schemas to BigQuery table definitions."""

    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(project=project_id)
        self.translation_errors: List[str] = []

    def translate_type(self, sf_type: str) -> Tuple[str, Optional[int], Optional[int]]:
        """Map a Snowflake type string to a BigQuery type."""
        # Strip precision/scale: NUMBER(38,0) -> NUMBER
        match = re.match(r"(\w+)(?:\((\d+)(?:,(\d+))?\))?", sf_type.upper().strip())
        if not match:
            logger.warning("Cannot parse type: %s, defaulting to STRING", sf_type)
            return "STRING", None, None

        base_type = match.group(1)
        precision = int(match.group(2)) if match.group(2) else None
        scale = int(match.group(3)) if match.group(3) else None

        # Special handling for NUMBER without fractional -> INT64
        if base_type in ("NUMBER", "DECIMAL", "NUMERIC") and scale == 0:
            return "INT64", precision, scale

        bq_type = TYPE_MAPPING.get(base_type, "STRING")
        if bq_type == "STRING" and base_type not in TYPE_MAPPING:
            self.translation_errors.append(
                f"Unknown Snowflake type '{sf_type}', mapped to STRING"
            )
        return bq_type, precision, scale

    def translate_column(self, col_info: dict) -> ColumnDefinition:
        """Translate a single column descriptor from Snowflake metadata."""
        bq_type, precision, scale = self.translate_type(col_info["data_type"])
        return ColumnDefinition(
            name=col_info["column_name"].lower(),
            sf_type=col_info["data_type"],
            bq_type=bq_type,
            nullable=col_info.get("is_nullable", "YES") == "YES",
            default=col_info.get("column_default"),
            comment=col_info.get("comment"),
            precision=precision,
            scale=scale,
        )

    def build_bq_schema(self, table_schema: TableSchema) -> List[bigquery.SchemaField]:
        """Convert TableSchema to list of BigQuery SchemaField objects."""
        fields = []
        for col in table_schema.columns:
            mode = "NULLABLE" if col.nullable else "REQUIRED"
            fields.append(
                bigquery.SchemaField(
                    name=col.name,
                    field_type=col.bq_type,
                    mode=mode,
                    description=col.comment or "",
                )
            )
        return fields

    def create_bq_table(
        self,
        table_schema: TableSchema,
        partition_col: Optional[str] = None,
        cluster_cols: Optional[List[str]] = None,
        exists_ok: bool = True,
    ) -> bigquery.Table:
        """Create a BigQuery table from a translated TableSchema."""
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_schema.table_name}"
        schema_fields = self.build_bq_schema(table_schema)

        table = bigquery.Table(table_ref, schema=schema_fields)

        # Apply partitioning
        if partition_col:
            table.time_partitioning = bigquery.TimePartitioning(
                field=partition_col,
                type_=bigquery.TimePartitioningType.DAY,
            )
            logger.info("Partition on column: %s", partition_col)

        # Apply clustering (max 4 columns in BQ)
        if cluster_cols:
            table.clustering_fields = cluster_cols[:4]
            logger.info("Cluster keys: %s", cluster_cols[:4])

        created_table = self.bq_client.create_table(table, exists_ok=exists_ok)
        logger.info("Created BQ table: %s", table_ref)
        return created_table

    def translate_and_create(
        self,
        snowflake_columns: List[dict],
        table_name: str,
        database: str = "",
        schema: str = "",
        partition_col: Optional[str] = None,
        cluster_cols: Optional[List[str]] = None,
    ) -> bigquery.Table:
        """End-to-end: translate Snowflake column metadata and create BQ table."""
        table_schema = TableSchema(
            database=database,
            schema=schema,
            table_name=table_name,
            columns=[self.translate_column(c) for c in snowflake_columns],
        )
        if self.translation_errors:
            logger.warning(
                "Translation warnings for %s: %s",
                table_name,
                self.translation_errors,
            )
        return self.create_bq_table(
            table_schema,
            partition_col=partition_col,
            cluster_cols=cluster_cols,
        )

    def export_schema_json(self, table_schema: TableSchema, output_path: str) -> None:
        """Export the translated schema to a JSON file for review."""
        schema_dict = [
            {
                "name": col.name,
                "type": col.bq_type,
                "mode": "NULLABLE" if col.nullable else "REQUIRED",
                "description": col.comment or "",
                "source_type": col.sf_type,
            }
            for col in table_schema.columns
        ]
        with open(output_path, "w") as f:
            json.dump(schema_dict, f, indent=2)
        logger.info("Schema exported to %s", output_path)


if __name__ == "__main__":
    # Example usage
    sample_columns = [
        {"column_name": "USER_ID", "data_type": "NUMBER(38,0)", "is_nullable": "NO"},
        {"column_name": "EMAIL", "data_type": "VARCHAR(255)", "is_nullable": "YES"},
        {"column_name": "CREATED_AT", "data_type": "TIMESTAMP_NTZ", "is_nullable": "YES"},
        {"column_name": "METADATA", "data_type": "VARIANT", "is_nullable": "YES"},
        {"column_name": "REVENUE", "data_type": "NUMBER(18,4)", "is_nullable": "YES"},
    ]

    translator = SnowflakeToBigQueryTranslator(
        project_id="my-gcp-project",
        dataset_id="migrated_dataset",
    )

    bq_table = translator.translate_and_create(
        snowflake_columns=sample_columns,
        table_name="users",
        partition_col="created_at",
        cluster_cols=["user_id"],
    )
    print(f"Created table: {bq_table.full_table_id}")
