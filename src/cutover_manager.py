"""Cutover Manager: orchestrates the dual-write cutover window,
gates the final switch behind validation, and supports rollback.

This is the module referenced by the original README but not yet
implemented.
"""

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class CutoverPhase(str, Enum):
    NOT_STARTED = "not_started"
    DUAL_WRITE = "dual_write"
    VALIDATING = "validating"
    SWITCHED = "switched"
    ROLLED_BACK = "rolled_back"


@dataclass
class CutoverStatus:
    table_name: str
    phase: CutoverPhase = CutoverPhase.NOT_STARTED
    dual_write_started_at: Optional[str] = None
    validation_passed: Optional[bool] = None
    switched_at: Optional[str] = None
    rolled_back_at: Optional[str] = None
    notes: List[str] = field(default_factory=list)


class CutoverManager:
    """Coordinates dual-write, a validation gate, and rollback for a
    zero-downtime Snowflake -> BigQuery cutover."""

    def __init__(self, state_path: str = "migration_state.json"):
        self.state_path = state_path
        self.statuses: Dict[str, CutoverStatus] = {}
        self._load_state()

    def _load_state(self) -> None:
        try:
            with open(self.state_path) as f:
                raw = json.load(f)
            for name, payload in raw.get("cutover", {}).items():
                payload["phase"] = CutoverPhase(payload["phase"])
                self.statuses[name] = CutoverStatus(**payload)
        except FileNotFoundError:
            logger.info("No existing cutover state at %s; starting fresh", self.state_path)

    def _save_state(self) -> None:
        existing: dict = {}
        try:
            with open(self.state_path) as f:
                existing = json.load(f)
        except FileNotFoundError:
            pass
        existing["cutover"] = {
            name: {**vars(status), "phase": status.phase.value}
            for name, status in self.statuses.items()
        }
        with open(self.state_path, "w") as f:
            json.dump(existing, f, indent=2, default=str)

    def _status(self, table_name: str) -> CutoverStatus:
        if table_name not in self.statuses:
            self.statuses[table_name] = CutoverStatus(table_name=table_name)
        return self.statuses[table_name]

    def start_dual_write(self, table_name: str) -> CutoverStatus:
        """Mark a table as entering the dual-write window. Idempotent — re-running
        on a table already in dual_write is a no-op."""
        status = self._status(table_name)
        if status.phase == CutoverPhase.NOT_STARTED:
            status.phase = CutoverPhase.DUAL_WRITE
            status.dual_write_started_at = datetime.utcnow().isoformat()
            status.notes.append("Entered dual-write window")
            self._save_state()
        return status

    def run_validation_gate(
        self, table_name: str, validate_fn: Callable[[str], bool]
    ) -> CutoverStatus:
        """Run the supplied validation callback; only a pass unlocks the switch."""
        status = self._status(table_name)
        status.phase = CutoverPhase.VALIDATING
        try:
            passed = validate_fn(table_name)
        except Exception as exc:  # noqa: BLE001 - any validation error blocks cutover
            logger.error("Validation gate raised for %s: %s", table_name, exc)
            passed = False
            status.notes.append(f"Validation error: {exc}")

        status.validation_passed = passed
        status.notes.append(f"Validation gate {'PASSED' if passed else 'FAILED'}")
        self._save_state()
        return status

    def switch_to_bigquery(self, table_name: str, force: bool = False) -> CutoverStatus:
        """Flip reads/writes to BigQuery. Refuses unless validation passed, unless forced."""
        status = self._status(table_name)
        if not force and not status.validation_passed:
            raise RuntimeError(
                f"Refusing cutover for {table_name}: validation gate has not passed"
            )
        status.phase = CutoverPhase.SWITCHED
        status.switched_at = datetime.utcnow().isoformat()
        status.notes.append("Switched primary to BigQuery")
        self._save_state()
        logger.info("Cutover complete for %s", table_name)
        return status

    def rollback(self, table_name: str, reason: str) -> CutoverStatus:
        """Revert to Snowflake as primary. Safe to call from any phase."""
        status = self._status(table_name)
        status.phase = CutoverPhase.ROLLED_BACK
        status.rolled_back_at = datetime.utcnow().isoformat()
        status.notes.append(f"Rolled back: {reason}")
        self._save_state()
        logger.warning("Rolled back cutover for %s: %s", table_name, reason)
        return status

    def cutover_table(
        self, table_name: str, validate_fn: Callable[[str], bool], force: bool = False
    ) -> CutoverStatus:
        """End-to-end: dual-write -> validate -> switch (or block)."""
        self.start_dual_write(table_name)
        status = self.run_validation_gate(table_name, validate_fn)
        if status.validation_passed or force:
            return self.switch_to_bigquery(table_name, force=force)
        logger.warning("Cutover blocked for %s pending validation", table_name)
        return status

    def generate_runbook(self) -> str:
        """Markdown runbook summarizing cutover status across all tables."""
        lines = ["# Cutover Runbook\n"]
        lines.append("| Table | Phase | Validation | Switched At | Notes |")
        lines.append("|-------|-------|------------|-------------|-------|")
        for status in self.statuses.values():
            lines.append(
                f"| {status.table_name} | {status.phase.value} | {status.validation_passed} "
                f"| {status.switched_at or '-'} | {'; '.join(status.notes)} |"
            )
        return "\n".join(lines)


if __name__ == "__main__":
    manager = CutoverManager(state_path="migration_state.json")
    status = manager.cutover_table(
        table_name="users",
        validate_fn=lambda _: True,
    )
    print(manager.generate_runbook())
