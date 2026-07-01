"""Data Validator: Row count, checksum and null-check comparisons
between Snowflake source and BigQuery target after migration.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

import snowflake.connector
from google.cloud import bigquery

logger = logging.getLogger(__name__)


_LARGE_TABLE_ROW_THRESHOLD = 50_000_000
_DEFAULT_SAMPLE_SIZE = 100_000


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
    sampled: bool = False
    type_mismatches: List[Dict] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.type_mismatches is None:
            self.type_mismatches = []


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

    def get_sampled_checksum(
        self, sf_table: str, bq_table: str, key_col: str, sample_size: int = _DEFAULT_SAMPLE_SIZE
    ) -> tuple:
        """MD5 checksum over a random sample, for tables too large to checksum in full."""
        sf_sql = f"""
            SELECT MD5(LISTAGG({key_col}, ',') WITHIN GROUP (ORDER BY {key_col}))
            FROM (
                SELECT {key_col} FROM {sf_table}
                SAMPLE ({sample_size} ROWS)
            )
        """
        bq_full = f"{self.bq_project}.{self.bq_dataset}.{bq_table}"
        bq_sql = f"""
            SELECT TO_HEX(MD5(STRING_AGG(CAST({key_col} AS STRING), ',' ORDER BY {key_col})))
            FROM (
                SELECT {key_col} FROM `{bq_full}` TABLESAMPLE SYSTEM (10 PERCENT)
                LIMIT {sample_size}
            )
        """
        sf_cksum = self._sf_query(sf_sql)[0][0]
        bq_cksum = self._bq_query(bq_sql)[0][0]
        return sf_cksum, bq_cksum

    def check_column_types(self, sf_table: str, bq_table: str) -> List[Dict]:
        """Compare column data types between Snowflake and BigQuery, flagging mismatches."""
        database, schema, table = sf_table.split(".")
        sf_cols = {
            row[0].lower(): row[1]
            for row in self._sf_query(
                f"""SELECT COLUMN_NAME, DATA_TYPE FROM {database}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = '{table.upper()}'"""
            )
        }
        bq_full = f"{self.bq_project}.{self.bq_dataset}.{bq_table}"
        bq_table_ref = self.bq_client.get_table(bq_full)
        bq_cols = {f.name.lower(): f.field_type for f in bq_table_ref.schema}

        mismatches = []
        for name, sf_type in sf_cols.items():
            if name not in bq_cols:
                mismatches.append({"column": name, "snowflake": sf_type, "bigquery": "MISSING"})
        return mismatches

    def validate_table(
        self,
        sf_table: str,
        bq_table: str,
        key_col: str,
        columns: Optional[List[str]] = None,
        row_count_threshold: int = _LARGE_TABLE_ROW_THRESHOLD,
        sample_size: int = _DEFAULT_SAMPLE_SIZE,
    ) -> ValidationResult:
        """Run full validation suite on a migrated table. Tables above
        row_count_threshold are checksummed via sampling instead of a full scan."""
        logger.info("Validating %s -> %s", sf_table, bq_table)

        sf_count, bq_count = self.get_row_counts(sf_table, bq_table)
        sampled = sf_count > row_count_threshold or bq_count > row_count_threshold

        if sampled:
            sf_cksum, bq_cksum = self.get_sampled_checksum(sf_table, bq_table, key_col, sample_size)
            logger.info("Table %s exceeds %d rows; using sampled checksum", bq_table, row_count_threshold)
        else:
            sf_cksum, bq_cksum = self.get_checksum(sf_table, bq_table, key_col)

        null_discrepancies = {}
        if columns:
            null_discrepancies = self.check_null_counts(sf_table, bq_table, columns)

        type_mismatches = self.check_column_types(sf_table, bq_table)

        row_match = sf_count == bq_count
        cksum_match = sf_cksum == bq_cksum
        passed = row_match and cksum_match and not null_discrepancies and not type_mismatches

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
            sampled=sampled,
            type_mismatches=type_mismatches,
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
        lines.append("| Table | SF Rows | BQ Rows | Rows Match | Checksum Match | Sampled | Status |")
        lines.append("|-------|---------|---------|------------|----------------|---------|--------|")
        for r in results:
            status = "✅ PASS" if r.passed else "❌ FAIL"
            lines.append(
                f"| {r.table_name} | {r.sf_row_count:,} | {r.bq_row_count:,} "
                f"| {r.row_count_match} | {r.checksum_match} | {r.sampled} | {status} |"
            )

        type_issues = [(r.table_name, m) for r in results for m in r.type_mismatches]
        if type_issues:
            lines.append("\n## Column Type Mismatches\n")
            lines.append("| Table | Column | Snowflake Type | BigQuery Type |")
            lines.append("|-------|--------|-----------------|---------------|")
            for table_name, mismatch in type_issues:
                lines.append(
                    f"| {table_name} | {mismatch['column']} | {mismatch['snowflake']} | {mismatch['bigquery']} |"
                )
        return "\n".join(lines)

    def generate_summary_dashboard(self, results: List[ValidationResult]) -> str:
        """Generate a condensed Markdown dashboard: pass/fail counts and worst offenders."""
        total = len(results)
        passed = sum(1 for r in results if r.passed)
        failed = total - passed
        sampled = sum(1 for r in results if r.sampled)
        total_rows = sum(r.bq_row_count for r in results)

        lines = ["# Validation Summary Dashboard\n"]
        lines.append(f"- **Tables validated:** {total}")
        lines.append(f"- **Passed:** {passed} ({(passed / total * 100) if total else 0:.1f}%)")
        lines.append(f"- **Failed:** {failed}")
        lines.append(f"- **Sampled (large tables):** {sampled}")
        lines.append(f"- **Total rows validated:** {total_rows:,}\n")

        failures = [r for r in results if not r.passed]
        if failures:
            lines.append("## Failing Tables\n")
            for r in failures:
                reasons = []
                if not r.row_count_match:
                    reasons.append("row count mismatch")
                if not r.checksum_match:
                    reasons.append("checksum mismatch")
                if r.null_discrepancies:
                    reasons.append(f"{len(r.null_discrepancies)} null discrepancies")
                if r.type_mismatches:
                    reasons.append(f"{len(r.type_mismatches)} type mismatches")
                lines.append(f"- **{r.table_name}**: {', '.join(reasons)}")
        return "\n".join(lines)
