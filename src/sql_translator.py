"""SQL Translator: batch-translates Snowflake SQL (views, stored
procedures, UDFs, queries) to BigQuery SQL using Google's BigQuery
Migration Service SQL translation API.
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from google.api_core.exceptions import GoogleAPICallError, ResourceExhausted
from google.cloud import bigquery_migration_v2 as migration_v2

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_BACKOFF_BASE_SECONDS = 2


@dataclass
class TranslationUnit:
    object_name: str
    object_type: str  # view, procedure, function, query
    source_sql: str


@dataclass
class TranslationResult:
    object_name: str
    object_type: str
    source_sql: str
    translated_sql: Optional[str]
    success: bool
    untranslated_tokens: List[str] = field(default_factory=list)
    error: Optional[str] = None


class SnowflakeSqlTranslator:
    """Wraps the BigQuery Migration Service SQL translation API for
    batch Snowflake -> BigQuery SQL translation."""

    def __init__(self, project_id: str, location: str = "us"):
        self.project_id = project_id
        self.location = location
        self.client = migration_v2.MigrationServiceClient()
        self.results: List[TranslationResult] = []

    def _translate_one(self, unit: TranslationUnit) -> TranslationResult:
        """Submit a single literal-SQL translation task with retry/backoff."""
        relative_path = f"{unit.object_name}.sql"
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                task = migration_v2.MigrationTask(
                    type_="Translation_Snowflake2BQ",
                    translation_config_details=migration_v2.TranslationConfigDetails(
                        source_dialect=migration_v2.Dialect(
                            snowflake_dialect=migration_v2.SnowflakeDialect()
                        ),
                        target_dialect=migration_v2.Dialect(
                            bigquery_dialect=migration_v2.BigQueryDialect()
                        ),
                    ),
                    translation_details=migration_v2.TranslationDetails(
                        source_target_mapping=[
                            migration_v2.SourceTargetMapping(
                                source_spec=migration_v2.SourceSpec(
                                    literal=migration_v2.Literal(
                                        literal_string=unit.source_sql,
                                        relative_path=relative_path,
                                    )
                                )
                            )
                        ],
                        target_return_literals=[relative_path],
                    ),
                )
                workflow = migration_v2.MigrationWorkflow(
                    display_name=f"translate-{unit.object_name}",
                    tasks={"translation-task": task},
                )
                request = migration_v2.CreateMigrationWorkflowRequest(
                    parent=f"projects/{self.project_id}/locations/{self.location}",
                    migration_workflow=workflow,
                )
                created = self.client.create_migration_workflow(request=request)
                translated_sql, issues = self._poll_workflow(created.name)
                return TranslationResult(
                    object_name=unit.object_name,
                    object_type=unit.object_type,
                    source_sql=unit.source_sql,
                    translated_sql=translated_sql,
                    success=translated_sql is not None,
                    untranslated_tokens=issues,
                )
            except ResourceExhausted as exc:
                wait = _BACKOFF_BASE_SECONDS ** attempt
                logger.warning(
                    "Rate limited translating %s (attempt %d/%d), backing off %ds",
                    unit.object_name, attempt, _MAX_RETRIES, wait,
                )
                time.sleep(wait)
            except GoogleAPICallError as exc:
                logger.error("Translation failed for %s: %s", unit.object_name, exc)
                return TranslationResult(
                    object_name=unit.object_name,
                    object_type=unit.object_type,
                    source_sql=unit.source_sql,
                    translated_sql=None,
                    success=False,
                    error=str(exc),
                )
        return TranslationResult(
            object_name=unit.object_name,
            object_type=unit.object_type,
            source_sql=unit.source_sql,
            translated_sql=None,
            success=False,
            error="Exhausted retries due to rate limiting",
        )

    def _poll_workflow(self, workflow_name: str, timeout_seconds: int = 300) -> tuple:
        """Poll a migration workflow until COMPLETED; return (sql, issues)."""
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            workflow = self.client.get_migration_workflow(name=workflow_name)
            if workflow.state == migration_v2.MigrationWorkflow.State.COMPLETED:
                return self._extract_output(workflow)
            if workflow.state == migration_v2.MigrationWorkflow.State.PAUSED:
                return None, ["Workflow paused unexpectedly"]
            time.sleep(2)
        return None, ["Translation workflow timed out"]

    @staticmethod
    def _extract_output(workflow) -> tuple:
        """Pull translated SQL + any reported issues from a completed workflow's tasks."""
        tasks = getattr(workflow, "tasks", {}) or {}
        sql_parts: List[str] = []
        issues: List[str] = []
        for task in tasks.values():
            if task.state == migration_v2.MigrationTask.State.FAILED:
                issues.append(str(task.processing_error) or f"Task {task.id} failed")
                continue
            translation_result = task.task_result.translation_task_result
            for literal in translation_result.translated_literals:
                sql_parts.append(literal.literal_string)
            for message in translation_result.report_log_messages:
                issues.append(message.message)
        return ("\n".join(sql_parts) or None), issues

    def translate_batch(self, units: List[TranslationUnit]) -> List[TranslationResult]:
        """Translate multiple SQL objects, preserving names and continuing past failures."""
        results = []
        for unit in units:
            logger.info("Translating %s (%s)", unit.object_name, unit.object_type)
            result = self._translate_one(unit)
            results.append(result)
        self.results.extend(results)
        return results

    def generate_issues_report(self, results: Optional[List[TranslationResult]] = None) -> str:
        """Markdown report of translation failures and untranslated tokens."""
        results = results if results is not None else self.results
        lines = ["# SQL Translation Issues Report\n"]
        failed = [r for r in results if not r.success]
        with_issues = [r for r in results if r.untranslated_tokens]

        lines.append(f"Total objects: {len(results)} | Failed: {len(failed)} | With issues: {len(with_issues)}\n")
        lines.append("| Object | Type | Status | Issues |")
        lines.append("|--------|------|--------|--------|")
        for r in results:
            status = "OK" if r.success else "FAILED"
            issues = "; ".join(r.untranslated_tokens) if r.untranslated_tokens else (r.error or "-")
            lines.append(f"| {r.object_name} | {r.object_type} | {status} | {issues} |")
        return "\n".join(lines)


if __name__ == "__main__":
    translator = SnowflakeSqlTranslator(project_id="my-gcp-project")
    units = [
        TranslationUnit(
            object_name="vw_active_users",
            object_type="view",
            source_sql="SELECT id, email FROM users WHERE deleted_at IS NULL",
        ),
    ]
    results = translator.translate_batch(units)
    print(translator.generate_issues_report(results))
