"""Data Validator: Row count, checksum and null-check comparisons
between Snowflake source and BigQuery target after migration.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

import snowflake.connector
from google.cloud import bigquery

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    table_name: str
    sf_row_count: int
    bq_row_count: int
    row_count_match: bool
    sf_checksum: Optional[str]
    bq_checksum: Optional[str]
    checksum_match: bool
    null_discrepancies: Dict[str, Dict]
    passed: bool


class MigrationValidator:
    """Validates data parity between Snowflake and BigQuery tables."""

    def __init__(
        self,
        sf_conn_params: dict,
        bq_project: str,
        bq_dataset: str,
    ):
        self.sf_conn = snowflake.connector.connect(**sf_conn_params)
        self.bq_client = bigquery.Client(project=bq_project)
        self.bq_dataset = bq_dataset
        self.bq_project = bq_project

    def _sf_query(self, sql: str) -> list:
        cur = self.sf_conn.cursor()
        cur.execute(sql)
        return cur.fetchall()

    def _bq_query(self, sql: str) -> list:
        return list(self.bq_client.query(sql).result())

    def get_row_counts(self, sf_table: str, bq_table: str) -> tuple:
        sf_count = self._sf_query(f"SELECT COUNT(*) FROM {sf_table}")[0][0]
        bq_full = f"{self.bq_project}.{self.bq_dataset}.{bq_table}"
        bq_count = self._bq_query(f"SELECT COUNT(*) as cnt FROM `{bq_full}`")[0]["cnt"]
        return sf_count, bq_count

    def get_checksum(self, sf_table: str, bq_table: str, key_col: str) -> tuple:
        """MD5 checksum on a key column for basic integrity check."""
        sf_sql = f"""
            SELECT MD5(LISTAGG({key_col}, ',') WITHIN GROUP (ORDER BY {key_col}))
            FROM (SELECT {key_col} FROM {sf_table} ORDER BY {key_col} LIMIT 10000)
        """
        bq_full = f"{self.bq_project}.{self.bq_dataset}.{bq_table}"
        bq_sql = f"""
            SELECT TO_HEX(MD5(STRING_AGG(CAST({key_col} AS STRING), ',' ORDER BY {key_col})))
            FROM (SELECT {key_col} FROM `{bq_full}` ORDER BY {key_col} LIMIT 10000)
        """
        sf_cksum = self._sf_query(sf_sql)[0][0]
        bq_cksum = self._bq_query(bq_sql)[0][0]
        return sf_cksum, bq_cksum

    def check_null_counts(self, sf_table: str, bq_table: str, columns: List[str]) -> dict:
        """Compare null counts per column between SF and BQ."""
        discrepancies = {}
        bq_full = f"{self.bq_project}.{self.bq_dataset}.{bq_table}"

        for col in columns:
            sf_nulls = self._sf_query(
                f"SELECT COUNT(*) FROM {sf_table} WHERE {col} IS NULL"
            )[0][0]
            bq_nulls = self._bq_query(
                f"SELECT COUNT(*) as cnt FROM `{bq_full}` WHERE {col} IS NULL"
            )[0]["cnt"]

            if sf_nulls != bq_nulls:
                discrepancies[col] = {"snowflake": sf_nulls, "bigquery": bq_nulls}

        return discrepancies

    def validate_table(
        self,
        sf_table: str,
        bq_table: str,
        key_col: str,
        columns: Optional[List[str]] = None,
    ) -> ValidationResult:
        """Run full validation suite on a migrated table."""
        logger.info("Validating %s -> %s", sf_table, bq_table)

        sf_count, bq_count = self.get_row_counts(sf_table, bq_table)
        sf_cksum, bq_cksum = self.get_checksum(sf_table, bq_table, key_col)
        null_discrepancies = {}
        if columns:
            null_discrepancies = self.check_null_counts(sf_table, bq_table, columns)

        row_match = sf_count == bq_count
        cksum_match = sf_cksum == bq_cksum
        passed = row_match and cksum_match and not null_discrepancies

        result = ValidationResult(
            table_name=bq_table,
            sf_row_count=sf_count,
            bq_row_count=bq_count,
            row_count_match=row_match,
            sf_checksum=sf_cksum,
            bq_checksum=bq_cksum,
            checksum_match=cksum_match,
            null_discrepancies=null_discrepancies,
            passed=passed,
        )

        status = "PASSED" if passed else "FAILED"
        logger.info("Validation %s for %s | rows: %d vs %d", status, bq_table, sf_count, bq_count)
        return result

    def validate_all_tables(
        self, table_pairs: List[Dict], key_col: str = "id"
    ) -> List[ValidationResult]:
        """Validate a list of {sf_table, bq_table} pairs."""
        results = []
        for pair in table_pairs:
            result = self.validate_table(
                sf_table=pair["sf_table"],
                bq_table=pair["bq_table"],
                key_col=pair.get("key_col", key_col),
                columns=pair.get("columns"),
            )
            results.append(result)

        failed = [r for r in results if not r.passed]
        logger.info(
            "Validation complete: %d/%d tables passed",
            len(results) - len(failed),
            len(results),
        )
        return results

    def generate_report(self, results: List[ValidationResult]) -> str:
        """Generate a Markdown validation report."""
        lines = ["# Migration Validation Report\n"]
        lines.append("| Table | SF Rows | BQ Rows | Rows Match | Checksum Match | Status |")
        lines.append("|-------|---------|---------|------------|----------------|--------|")
        for r in results:
            status = "✅ PASS" if r.passed else "❌ FAIL"
            lines.append(
                f"| {r.table_name} | {r.sf_row_count:,} | {r.bq_row_count:,} "
                f"| {r.row_count_match} | {r.checksum_match} | {status} |"
            )
        return "\n".join(lines)
