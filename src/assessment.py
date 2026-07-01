"""Assessment & Discovery: inventories a Snowflake account and produces
a migration wave plan ranked by dependency and size.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional

import snowflake.connector

logger = logging.getLogger(__name__)

# Heuristic complexity weights
_COMPLEXITY_THRESHOLDS = (
    (1_000_000_000, "high"),
    (50_000_000, "medium"),
)


@dataclass
class ObjectInventory:
    object_type: str  # table, view, procedure, function, task, stream
    database: str
    schema: str
    name: str
    row_count: int = 0
    bytes_size: int = 0
    complexity: str = "low"
    depends_on: List[str] = field(default_factory=list)


@dataclass
class MigrationWave:
    wave_number: int
    objects: List[str] = field(default_factory=list)
    total_rows: int = 0
    total_bytes: int = 0


class SnowflakeAssessor:
    """Discovers Snowflake objects and builds a migration wave plan."""

    def __init__(self, sf_conn_params: dict):
        self.sf_conn = snowflake.connector.connect(**sf_conn_params)
        self.inventory: List[ObjectInventory] = []

    def _query(self, sql: str) -> List[dict]:
        cur = self.sf_conn.cursor(snowflake.connector.DictCursor)
        cur.execute(sql)
        return cur.fetchall()

    def discover_tables(self, database: str, schema: Optional[str] = None) -> List[ObjectInventory]:
        """Inventory tables with row count and byte size from INFORMATION_SCHEMA."""
        where = f"AND TABLE_SCHEMA = '{schema}'" if schema else ""
        sql = f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, ROW_COUNT, BYTES
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE' {where}
        """
        rows = self._query(sql)
        objects = [
            ObjectInventory(
                object_type="table",
                database=database,
                schema=r["TABLE_SCHEMA"],
                name=r["TABLE_NAME"],
                row_count=r["ROW_COUNT"] or 0,
                bytes_size=r["BYTES"] or 0,
                complexity=self._estimate_complexity(r["ROW_COUNT"] or 0),
            )
            for r in rows
        ]
        self.inventory.extend(objects)
        return objects

    def discover_views(self, database: str, schema: Optional[str] = None) -> List[ObjectInventory]:
        where = f"AND TABLE_SCHEMA = '{schema}'" if schema else ""
        sql = f"""
            SELECT TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION
            FROM {database}.INFORMATION_SCHEMA.VIEWS
            WHERE 1=1 {where}
        """
        rows = self._query(sql)
        objects = [
            ObjectInventory(
                object_type="view",
                database=database,
                schema=r["TABLE_SCHEMA"],
                name=r["TABLE_NAME"],
                depends_on=self._extract_dependencies(r.get("VIEW_DEFINITION", "")),
            )
            for r in rows
        ]
        self.inventory.extend(objects)
        return objects

    def discover_routines(self, database: str, schema: Optional[str] = None) -> List[ObjectInventory]:
        """Stored procedures and UDFs."""
        where = f"AND PROCEDURE_SCHEMA = '{schema}'" if schema else ""
        sql = f"""
            SELECT PROCEDURE_SCHEMA, PROCEDURE_NAME, PROCEDURE_LANGUAGE
            FROM {database}.INFORMATION_SCHEMA.PROCEDURES
            WHERE 1=1 {where}
        """
        rows = self._query(sql)
        objects = [
            ObjectInventory(
                object_type="procedure",
                database=database,
                schema=r["PROCEDURE_SCHEMA"],
                name=r["PROCEDURE_NAME"],
                complexity="high" if r.get("PROCEDURE_LANGUAGE") != "SQL" else "medium",
            )
            for r in rows
        ]
        self.inventory.extend(objects)
        return objects

    def discover_tasks_and_streams(self, database: str, schema: Optional[str] = None) -> List[ObjectInventory]:
        where = f"AND SCHEMA_NAME = '{schema}'" if schema else ""
        objects: List[ObjectInventory] = []
        for kind, table in (("task", "TASKS"), ("stream", "STREAMS")):
            try:
                rows = self._query(f"SHOW {table} IN DATABASE {database}")
            except Exception as exc:  # noqa: BLE001 - best-effort discovery
                logger.warning("Could not list %s for %s: %s", table, database, exc)
                continue
            for r in rows:
                objects.append(
                    ObjectInventory(
                        object_type=kind,
                        database=database,
                        schema=r.get("schema_name", schema or ""),
                        name=r.get("name", ""),
                        complexity="high" if kind == "task" else "medium",
                    )
                )
        self.inventory.extend(objects)
        return objects

    @staticmethod
    def _estimate_complexity(row_count: int) -> str:
        for threshold, label in _COMPLEXITY_THRESHOLDS:
            if row_count >= threshold:
                return label
        return "low"

    @staticmethod
    def _extract_dependencies(view_sql: str) -> List[str]:
        """Best-effort extraction of referenced object names from FROM/JOIN clauses."""
        import re

        tokens = re.findall(r"(?:FROM|JOIN)\s+([A-Za-z0-9_.\"]+)", view_sql or "", re.IGNORECASE)
        return sorted({t.strip('"').upper() for t in tokens})

    def build_wave_plan(self, max_waves: int = 5) -> List[MigrationWave]:
        """Group objects into waves: independent/small objects first,
        large or dependency-heavy objects in later waves."""
        ordered = sorted(
            self.inventory,
            key=lambda o: (len(o.depends_on), o.bytes_size),
        )
        waves: List[MigrationWave] = [MigrationWave(wave_number=i + 1) for i in range(max_waves)]
        for idx, obj in enumerate(ordered):
            wave = waves[idx % max_waves]
            wave.objects.append(f"{obj.database}.{obj.schema}.{obj.name}")
            wave.total_rows += obj.row_count
            wave.total_bytes += obj.bytes_size
        return [w for w in waves if w.objects]

    def generate_report(self, waves: List[MigrationWave]) -> str:
        """Markdown assessment report covering inventory and wave plan."""
        lines = ["# Migration Assessment Report\n"]
        lines.append(f"Total objects discovered: {len(self.inventory)}\n")
        lines.append("| Type | Database | Schema | Name | Rows | Bytes | Complexity |")
        lines.append("|------|----------|--------|------|------|-------|------------|")
        for o in self.inventory:
            lines.append(
                f"| {o.object_type} | {o.database} | {o.schema} | {o.name} "
                f"| {o.row_count:,} | {o.bytes_size:,} | {o.complexity} |"
            )

        lines.append("\n## Migration Wave Plan\n")
        for wave in waves:
            lines.append(f"### Wave {wave.wave_number}")
            lines.append(f"- Objects: {len(wave.objects)}")
            lines.append(f"- Total rows: {wave.total_rows:,}")
            lines.append(f"- Total bytes: {wave.total_bytes:,}")
            for obj_name in wave.objects:
                lines.append(f"  - {obj_name}")
            lines.append("")
        return "\n".join(lines)

    def export_json(self, waves: List[MigrationWave], output_path: str) -> None:
        """Write inventory + wave plan to a JSON file."""
        payload = {
            "inventory": [vars(o) for o in self.inventory],
            "waves": [vars(w) for w in waves],
        }
        with open(output_path, "w") as f:
            json.dump(payload, f, indent=2)
        logger.info("Assessment exported to %s", output_path)


if __name__ == "__main__":
    assessor = SnowflakeAssessor(
        sf_conn_params={
            "account": "myaccount",
            "user": "myuser",
            "password": "mypassword",
        }
    )
    assessor.discover_tables(database="ANALYTICS")
    assessor.discover_views(database="ANALYTICS")
    assessor.discover_routines(database="ANALYTICS")
    assessor.discover_tasks_and_streams(database="ANALYTICS")
    plan = assessor.build_wave_plan()
    print(assessor.generate_report(plan))
