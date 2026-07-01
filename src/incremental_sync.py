"""Incremental Sync: watermark/CDC-based merge of Snowflake changes
into BigQuery target tables. Falls back to full reload when a table
has no usable watermark column.
"""

import logging
from dataclasses import dataclass
from typing import List, Optional

import snowflake.connector
from google.cloud import bigquery

logger = logging.getLogger(__name__)


@dataclass
class SyncResult:
    table_name: str
    strategy: str  # watermark, stream, full_reload
    rows_synced: int
    high_watermark: Optional[str] = None
    warning: Optional[str] = None


class IncrementalSyncer:
    """Syncs incremental changes from Snowflake into BigQuery using a
    watermark column, a Snowflake STREAM, or a full reload fallback."""

    def __init__(self, sf_conn_params: dict, bq_project: str, bq_dataset: str):
        self.sf_conn = snowflake.connector.connect(**sf_conn_params)
        self.bq_client = bigquery.Client(project=bq_project)
        self.bq_project = bq_project
        self.bq_dataset = bq_dataset

    def _sf_query(self, sql: str) -> list:
        cur = self.sf_conn.cursor(snowflake.connector.DictCursor)
        cur.execute(sql)
        return cur.fetchall()

    def get_last_watermark(self, bq_table: str, watermark_col: str) -> Optional[str]:
        """Read the current max watermark already present in BigQuery."""
        table_ref = f"{self.bq_project}.{self.bq_dataset}.{bq_table}"
        rows = list(
            self.bq_client.query(
                f"SELECT MAX({watermark_col}) AS wm FROM `{table_ref}`"
            ).result()
        )
        return str(rows[0]["wm"]) if rows and rows[0]["wm"] is not None else None

    def sync_via_watermark(
        self,
        sf_table: str,
        bq_table: str,
        key_col: str,
        watermark_col: str,
    ) -> SyncResult:
        """Pull rows newer than BigQuery's current max watermark and MERGE them in."""
        last_wm = self.get_last_watermark(bq_table, watermark_col)
        where_clause = f"WHERE {watermark_col} > '{last_wm}'" if last_wm else ""

        changed_rows = self._sf_query(f"SELECT * FROM {sf_table} {where_clause}")
        if not changed_rows:
            return SyncResult(table_name=bq_table, strategy="watermark", rows_synced=0, high_watermark=last_wm)

        self._merge_rows_into_bq(bq_table, changed_rows, key_col)
        new_wm = max(str(r[watermark_col.upper()]) for r in changed_rows)
        return SyncResult(
            table_name=bq_table,
            strategy="watermark",
            rows_synced=len(changed_rows),
            high_watermark=new_wm,
        )

    def sync_via_stream(self, stream_name: str, sf_table: str, bq_table: str, key_col: str) -> SyncResult:
        """Consume a Snowflake STREAM (CDC) and MERGE captured changes into BigQuery."""
        changes = self._sf_query(f"SELECT * FROM {stream_name}")
        if not changes:
            return SyncResult(table_name=bq_table, strategy="stream", rows_synced=0)

        deletes = [r for r in changes if r.get("METADATA$ACTION") == "DELETE"]
        upserts = [r for r in changes if r.get("METADATA$ACTION") != "DELETE"]

        if upserts:
            self._merge_rows_into_bq(bq_table, upserts, key_col)
        if deletes:
            self._delete_rows_from_bq(bq_table, deletes, key_col)

        return SyncResult(table_name=bq_table, strategy="stream", rows_synced=len(changes))

    def full_reload(self, sf_table: str, bq_table: str) -> SyncResult:
        """Fallback strategy when no watermark/stream is available: truncate + reload."""
        rows = self._sf_query(f"SELECT * FROM {sf_table}")
        table_ref = f"{self.bq_project}.{self.bq_dataset}.{bq_table}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        load_job = self.bq_client.load_table_from_json(rows, table_ref, job_config=job_config)
        load_job.result()
        return SyncResult(
            table_name=bq_table,
            strategy="full_reload",
            rows_synced=len(rows),
            warning="No watermark column available; performed full reload",
        )

    def sync_table(
        self,
        sf_table: str,
        bq_table: str,
        key_col: str,
        watermark_col: Optional[str] = None,
        stream_name: Optional[str] = None,
    ) -> SyncResult:
        """Idempotent dispatch: prefer STREAM, then watermark, then full reload."""
        if stream_name:
            return self.sync_via_stream(stream_name, sf_table, bq_table, key_col)
        if watermark_col:
            return self.sync_via_watermark(sf_table, bq_table, key_col, watermark_col)
        logger.warning("No watermark or stream for %s; falling back to full reload", bq_table)
        return self.full_reload(sf_table, bq_table)

    def _merge_rows_into_bq(self, bq_table: str, rows: List[dict], key_col: str) -> None:
        """Stage rows into a temp table then MERGE into the target — idempotent on re-run."""
        table_ref = f"{self.bq_project}.{self.bq_dataset}.{bq_table}"
        staging_table = f"{table_ref}_staging_tmp"

        normalized = [{k.lower(): v for k, v in row.items()} for row in rows]
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
        self.bq_client.load_table_from_json(normalized, staging_table, job_config=job_config).result()

        target = self.bq_client.get_table(table_ref)
        columns = [f.name for f in target.schema]
        update_clause = ", ".join(f"T.{c} = S.{c}" for c in columns if c != key_col)
        insert_cols = ", ".join(columns)
        insert_vals = ", ".join(f"S.{c}" for c in columns)

        merge_sql = f"""
            MERGE `{table_ref}` T
            USING `{staging_table}` S
            ON T.{key_col} = S.{key_col}
            WHEN MATCHED THEN UPDATE SET {update_clause}
            WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
        self.bq_client.query(merge_sql).result()
        self.bq_client.delete_table(staging_table, not_found_ok=True)
        logger.info("Merged %d rows into %s", len(rows), bq_table)

    def _delete_rows_from_bq(self, bq_table: str, rows: List[dict], key_col: str) -> None:
        table_ref = f"{self.bq_project}.{self.bq_dataset}.{bq_table}"
        keys = [str(r[key_col.upper()]) for r in rows]
        keys_literal = ", ".join(f"'{k}'" for k in keys)
        self.bq_client.query(
            f"DELETE FROM `{table_ref}` WHERE CAST({key_col} AS STRING) IN ({keys_literal})"
        ).result()
        logger.info("Deleted %d rows from %s", len(rows), bq_table)


if __name__ == "__main__":
    syncer = IncrementalSyncer(
        sf_conn_params={"account": "myaccount", "user": "myuser", "password": "mypassword"},
        bq_project="my-gcp-project",
        bq_dataset="migrated_dataset",
    )
    result = syncer.sync_table(
        sf_table="analytics.public.orders",
        bq_table="orders",
        key_col="order_id",
        watermark_col="updated_at",
    )
    print(result)
