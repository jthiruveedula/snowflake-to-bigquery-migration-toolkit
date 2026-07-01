"""Migrator: loads migration_config.yaml, builds the connection
params for each module, and runs phases per wave with state
persisted to migration_state.json so any phase is safe to re-run.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

DEFAULT_STATE_PATH = "migration_state.json"


@dataclass
class TableConfig:
    sf_table: str
    bq_table: str
    key_col: str
    watermark_col: Optional[str]
    wave: int


@dataclass
class MigrationConfig:
    sf_account: str
    sf_user: str
    sf_password: str
    sf_database: str
    sf_schemas: List[str]
    bq_project: str
    bq_dataset: str
    bq_location: str
    gcs_bucket: str
    tables: List[TableConfig]
    dts_schedule: str
    file_format: str
    state_file: str
    require_validation_pass: bool

    @property
    def sf_conn_params(self) -> dict:
        return {"account": self.sf_account, "user": self.sf_user, "password": self.sf_password}

    def tables_in_wave(self, wave: int) -> List[TableConfig]:
        return [t for t in self.tables if t.wave == wave]

    @property
    def waves(self) -> List[int]:
        return sorted({t.wave for t in self.tables})


def load_config(config_path: str) -> MigrationConfig:
    """Load and resolve migration_config.yaml, pulling secrets from env vars."""
    with open(config_path) as f:
        raw = yaml.safe_load(f)

    sf = raw["snowflake"]
    bq = raw["bigquery"]
    gcs = raw["gcs"]
    transfer = raw.get("transfer", {})
    cutover = raw.get("cutover", {})

    def _env(var_name: str) -> str:
        value = os.environ.get(var_name)
        if not value:
            raise EnvironmentError(f"Required environment variable not set: {var_name}")
        return value

    tables = [
        TableConfig(
            sf_table=t["sf_table"],
            bq_table=t["bq_table"],
            key_col=t["key_col"],
            watermark_col=t.get("watermark_col"),
            wave=t.get("wave", 1),
        )
        for t in raw.get("tables", [])
    ]

    return MigrationConfig(
        sf_account=_env(sf["account_env"]),
        sf_user=_env(sf["user_env"]),
        sf_password=_env(sf["password_env"]),
        sf_database=sf["database"],
        sf_schemas=sf.get("schemas", []),
        bq_project=_env(bq["project_id_env"]),
        bq_dataset=bq["dataset"],
        bq_location=bq.get("location", "US"),
        gcs_bucket=_env(gcs["bucket_env"]),
        tables=tables,
        dts_schedule=transfer.get("dts_schedule", "every 24 hours"),
        file_format=transfer.get("file_format", "PARQUET"),
        state_file=cutover.get("state_file", DEFAULT_STATE_PATH),
        require_validation_pass=cutover.get("require_validation_pass", True),
    )


class MigrationState:
    """Tracks which phase has completed for each table, so the CLI is safe to re-run."""

    def __init__(self, state_path: str = DEFAULT_STATE_PATH):
        self.state_path = state_path
        self.data: Dict[str, Dict] = self._load()

    def _load(self) -> dict:
        try:
            with open(self.state_path) as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def save(self) -> None:
        with open(self.state_path, "w") as f:
            json.dump(self.data, f, indent=2, default=str)

    def mark_complete(self, phase: str, table_name: str) -> None:
        self.data.setdefault(phase, {})[table_name] = "completed"
        self.save()

    def is_complete(self, phase: str, table_name: str) -> bool:
        return self.data.get(phase, {}).get(table_name) == "completed"


class Migrator:
    """Coordinates assessment, translation, schema creation, transfer,
    sync, validation, and cutover across migration waves."""

    def __init__(self, config: MigrationConfig):
        self.config = config
        self.state = MigrationState(config.state_file)

    def run_phase_for_wave(self, phase: str, wave: int, phase_fn) -> List[str]:
        """Run phase_fn(table_config) for every table in a wave, skipping
        tables already marked complete for this phase."""
        completed = []
        for table in self.config.tables_in_wave(wave):
            if self.state.is_complete(phase, table.bq_table):
                logger.info("Skipping %s for %s (already complete)", phase, table.bq_table)
                completed.append(table.bq_table)
                continue
            phase_fn(table)
            self.state.mark_complete(phase, table.bq_table)
            completed.append(table.bq_table)
        return completed
