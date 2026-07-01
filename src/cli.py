"""Click-based CLI tying together every phase of the Snowflake -> BigQuery
migration toolkit. Usable as `python -m src.cli <command>`.
"""

import logging

import click

from src.assessment import SnowflakeAssessor
from src.cutover_manager import CutoverManager
from src.data_transfer import DataTransferOrchestrator
from src.data_validator import MigrationValidator
from src.incremental_sync import IncrementalSyncer
from src.migrator import Migrator, load_config
from src.schema_translator import SnowflakeToBigQueryTranslator
from src.sql_translator import SnowflakeSqlTranslator, TranslationUnit

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.group()
@click.option("--config", "config_path", default="config/migration_config.yaml", show_default=True)
@click.pass_context
def cli(ctx: click.Context, config_path: str) -> None:
    """Snowflake to BigQuery migration toolkit."""
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path


@cli.command()
@click.option("--output", default="assessment_report.md", show_default=True)
@click.pass_context
def assess(ctx: click.Context, output: str) -> None:
    """Inventory the Snowflake account and write an assessment report."""
    config = load_config(ctx.obj["config_path"])
    assessor = SnowflakeAssessor(sf_conn_params=config.sf_conn_params)
    for schema in config.sf_schemas:
        assessor.discover_tables(config.sf_database, schema)
        assessor.discover_views(config.sf_database, schema)
        assessor.discover_routines(config.sf_database, schema)
        assessor.discover_tasks_and_streams(config.sf_database, schema)
    waves = assessor.build_wave_plan()
    report = assessor.generate_report(waves)
    with open(output, "w") as f:
        f.write(report)
    click.echo(f"Assessment report written to {output}")


@cli.command("translate-sql")
@click.option("--output", default="translation_issues.md", show_default=True)
@click.pass_context
def translate_sql(ctx: click.Context, output: str) -> None:
    """Translate Snowflake SQL objects to BigQuery SQL via the Migration Service API."""
    config = load_config(ctx.obj["config_path"])
    translator = SnowflakeSqlTranslator(project_id=config.bq_project, location=config.bq_location)
    units = [
        TranslationUnit(object_name=t.bq_table, object_type="table", source_sql=f"SELECT * FROM {t.sf_table}")
        for t in config.tables
    ]
    results = translator.translate_batch(units)
    report = translator.generate_issues_report(results)
    with open(output, "w") as f:
        f.write(report)
    click.echo(f"Translation issues report written to {output}")


@cli.command("create-schema")
@click.pass_context
def create_schema(ctx: click.Context) -> None:
    """Translate DDL and create BigQuery tables for every configured table."""
    config = load_config(ctx.obj["config_path"])
    translator = SnowflakeToBigQueryTranslator(project_id=config.bq_project, dataset_id=config.bq_dataset)
    migrator = Migrator(config)

    for wave in migrator.config.waves:
        click.echo(f"-- Wave {wave} --")

        def _create(table) -> None:
            columns = translator.fetch_snowflake_columns(table.sf_table)
            translator.translate_and_create(
                snowflake_columns=columns,
                table_name=table.bq_table,
            )

        created = migrator.run_phase_for_wave("create-schema", wave, _create)
        click.echo(f"Created/verified tables: {', '.join(created)}")


@cli.command()
@click.pass_context
def transfer(ctx: click.Context) -> None:
    """Bulk-load every configured table from Snowflake into BigQuery via GCS staging."""
    config = load_config(ctx.obj["config_path"])
    orchestrator = DataTransferOrchestrator(
        sf_conn_params=config.sf_conn_params,
        gcp_project=config.bq_project,
        gcs_bucket=config.gcs_bucket,
        bq_dataset=config.bq_dataset,
    )
    migrator = Migrator(config)
    for wave in migrator.config.waves:
        click.echo(f"-- Wave {wave} --")
        migrator.run_phase_for_wave(
            "transfer",
            wave,
            lambda t: orchestrator.backfill_table(t.sf_table, t.bq_table),
        )
    click.echo(orchestrator.progress_report())


@cli.command()
@click.pass_context
def sync(ctx: click.Context) -> None:
    """Run incremental sync for every configured table."""
    config = load_config(ctx.obj["config_path"])
    syncer = IncrementalSyncer(
        sf_conn_params=config.sf_conn_params,
        bq_project=config.bq_project,
        bq_dataset=config.bq_dataset,
    )
    for table in config.tables:
        result = syncer.sync_table(
            sf_table=table.sf_table,
            bq_table=table.bq_table,
            key_col=table.key_col,
            watermark_col=table.watermark_col,
        )
        click.echo(f"{result.table_name}: {result.strategy}, {result.rows_synced} rows synced")


@cli.command()
@click.pass_context
def validate(ctx: click.Context) -> None:
    """Validate row counts, checksums, and null counts for every configured table."""
    config = load_config(ctx.obj["config_path"])
    validator = MigrationValidator(
        sf_conn_params=config.sf_conn_params,
        bq_project=config.bq_project,
        bq_dataset=config.bq_dataset,
    )
    pairs = [
        {"sf_table": t.sf_table, "bq_table": t.bq_table, "key_col": t.key_col}
        for t in config.tables
    ]
    results = validator.validate_all_tables(pairs)
    report = validator.generate_report(results)
    with open("validation_report.md", "w") as f:
        f.write(report)
    click.echo("Validation report written to validation_report.md")


@cli.command()
@click.option("--force", is_flag=True, default=False, help="Switch even if validation has not passed.")
@click.pass_context
def cutover(ctx: click.Context, force: bool) -> None:
    """Run the dual-write cutover with a validation gate for every configured table."""
    config = load_config(ctx.obj["config_path"])
    validator = MigrationValidator(
        sf_conn_params=config.sf_conn_params,
        bq_project=config.bq_project,
        bq_dataset=config.bq_dataset,
    )
    manager = CutoverManager(state_path=config.state_file)

    for table in config.tables:
        def _validate(_table_name: str, t=table) -> bool:
            result = validator.validate_table(sf_table=t.sf_table, bq_table=t.bq_table, key_col=t.key_col)
            return result.passed

        status = manager.cutover_table(table.bq_table, _validate, force=force)
        click.echo(f"{table.bq_table}: {status.phase.value}")

    with open("cutover_runbook.md", "w") as f:
        f.write(manager.generate_runbook())
    click.echo("Cutover runbook written to cutover_runbook.md")


if __name__ == "__main__":
    cli(obj={})
