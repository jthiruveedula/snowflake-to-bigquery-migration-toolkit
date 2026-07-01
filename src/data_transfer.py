"""Data Transfer: bulk loads Snowflake tables into BigQuery via
GCS staging + load jobs, and configures BigQuery Data Transfer
Service (DTS) for recurring/scheduled syncs.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import snowflake.connector
from google.cloud import bigquery
from google.cloud import bigquery_datatransfer_v1 as datatransfer
from google.cloud import storage

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BACKOFF_BASE_SECONDS = 5


@dataclass
class TransferProgress:
    table_name: str
    status: str  # pending, exporting, loading, completed, failed
    rows_transferred: int = 0
    attempts: int = 0
    error: Optional[str] = None


@dataclass
class TransferState:
    progress: Dict[str, TransferProgress] = field(default_factory=dict)

    def get_or_create(self, table_name: str) -> TransferProgress:
        if table_name not in self.progress:
            self.progress[table_name] = TransferProgress(table_name=table_name, status="pending")
        return self.progress[table_name]


class DataTransferOrchestrator:
    """Bulk loads Snowflake tables to BigQuery via GCS staging,
    and configures BigQuery DTS for recurring incremental loads."""

    def __init__(self, sf_conn_params: dict, gcp_project: str, gcs_bucket: str, bq_dataset: str):
        self.sf_conn = snowflake.connector.connect(**sf_conn_params)
        self.gcp_project = gcp_project
        self.gcs_bucket = gcs_bucket
        self.bq_dataset = bq_dataset
        self.storage_client = storage.Client(project=gcp_project)
        self.bq_client = bigquery.Client(project=gcp_project)
        self.dts_client = datatransfer.DataTransferServiceClient()
        self.state = TransferState()

    def export_table_to_gcs(self, sf_table: str, stage_path: str, file_format: str = "PARQUET") -> str:
        """Export a Snowflake table to GCS using a Snowflake external stage / COPY INTO."""
        gcs_uri = f"gcs://{self.gcs_bucket}/{stage_path}/"
        cur = self.sf_conn.cursor()
        cur.execute(
            f"""
            COPY INTO '{gcs_uri}'
            FROM {sf_table}
            FILE_FORMAT = (TYPE = {file_format})
            HEADER = TRUE
            OVERWRITE = TRUE
            """
        )
        logger.info("Exported %s to %s", sf_table, gcs_uri)
        return gcs_uri

    def load_gcs_to_bq(
        self,
        gcs_uri_pattern: str,
        bq_table: str,
        file_format: str = "PARQUET",
        write_disposition: str = "WRITE_TRUNCATE",
    ) -> int:
        """Load staged GCS files into a BigQuery table via a load job."""
        source_format = {
            "PARQUET": bigquery.SourceFormat.PARQUET,
            "CSV": bigquery.SourceFormat.CSV,
        }[file_format]

        job_config = bigquery.LoadJobConfig(
            source_format=source_format,
            write_disposition=write_disposition,
            autodetect=True,
        )
        table_ref = f"{self.gcp_project}.{self.bq_dataset}.{bq_table}"
        gcs_https_uri = gcs_uri_pattern.replace("gcs://", "gs://") + "*.parquet"

        load_job = self.bq_client.load_table_from_uri(
            gcs_https_uri, table_ref, job_config=job_config
        )
        load_job.result()
        destination = self.bq_client.get_table(table_ref)
        logger.info("Loaded %d rows into %s", destination.num_rows, table_ref)
        return destination.num_rows

    def backfill_table(self, sf_table: str, bq_table: str, stage_path: Optional[str] = None) -> TransferProgress:
        """One-time full backfill: export -> load, with retry/resumability."""
        stage_path = stage_path or f"backfill/{bq_table}"
        progress = self.state.get_or_create(bq_table)

        for attempt in range(1, _MAX_RETRIES + 1):
            progress.attempts = attempt
            try:
                progress.status = "exporting"
                gcs_uri = self.export_table_to_gcs(sf_table, stage_path)

                progress.status = "loading"
                rows = self.load_gcs_to_bq(gcs_uri, bq_table)

                progress.status = "completed"
                progress.rows_transferred = rows
                return progress
            except Exception as exc:  # noqa: BLE001 - retry on any transient failure
                wait = _BACKOFF_BASE_SECONDS * attempt
                progress.error = str(exc)
                logger.warning(
                    "Backfill attempt %d/%d failed for %s: %s. Retrying in %ds",
                    attempt, _MAX_RETRIES, bq_table, exc, wait,
                )
                time.sleep(wait)

        progress.status = "failed"
        logger.error("Backfill permanently failed for %s after %d attempts", bq_table, _MAX_RETRIES)
        return progress

    def backfill_all(self, table_pairs: List[Dict[str, str]]) -> List[TransferProgress]:
        """Backfill a list of {sf_table, bq_table} pairs, continuing past per-table failures."""
        results = []
        for pair in table_pairs:
            result = self.backfill_table(pair["sf_table"], pair["bq_table"], pair.get("stage_path"))
            results.append(result)
        return results

    def create_dts_transfer_config(
        self,
        display_name: str,
        gcs_uri_pattern: str,
        bq_table: str,
        schedule: str = "every 24 hours",
        data_source_id: str = "google_cloud_storage",
    ) -> datatransfer.TransferConfig:
        """Configure a recurring BigQuery DTS transfer from GCS to BigQuery."""
        parent = self.dts_client.common_project_path(self.gcp_project)
        transfer_config = datatransfer.TransferConfig(
            destination_dataset_id=self.bq_dataset,
            display_name=display_name,
            data_source_id=data_source_id,
            schedule=schedule,
            params={
                "data_path_template": gcs_uri_pattern,
                "destination_table_name_template": bq_table,
                "file_format": "PARQUET",
                "write_disposition": "APPEND",
            },
        )
        created = self.dts_client.create_transfer_config(
            parent=parent, transfer_config=transfer_config
        )
        logger.info("Created DTS transfer config: %s", created.name)
        return created

    def progress_report(self) -> str:
        """Markdown per-table transfer progress report."""
        lines = ["# Data Transfer Progress\n"]
        lines.append("| Table | Status | Rows | Attempts | Error |")
        lines.append("|-------|--------|------|----------|-------|")
        for p in self.state.progress.values():
            lines.append(
                f"| {p.table_name} | {p.status} | {p.rows_transferred:,} "
                f"| {p.attempts} | {p.error or '-'} |"
            )
        return "\n".join(lines)


if __name__ == "__main__":
    orchestrator = DataTransferOrchestrator(
        sf_conn_params={"account": "myaccount", "user": "myuser", "password": "mypassword"},
        gcp_project="my-gcp-project",
        gcs_bucket="my-migration-bucket",
        bq_dataset="migrated_dataset",
    )
    results = orchestrator.backfill_all(
        [{"sf_table": "analytics.public.users", "bq_table": "users"}]
    )
    print(orchestrator.progress_report())
