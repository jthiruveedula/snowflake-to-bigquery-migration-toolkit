import json

from src.cutover_manager import CutoverManager, CutoverPhase


def test_start_dual_write_is_idempotent(tmp_path):
    state_file = tmp_path / "state.json"
    manager = CutoverManager(state_path=str(state_file))

    first = manager.start_dual_write("users")
    started_at = first.dual_write_started_at
    second = manager.start_dual_write("users")

    assert second.phase == CutoverPhase.DUAL_WRITE
    assert second.dual_write_started_at == started_at


def test_validation_gate_blocks_switch(tmp_path):
    manager = CutoverManager(state_path=str(tmp_path / "state.json"))
    manager.start_dual_write("orders")
    manager.run_validation_gate("orders", lambda _: False)

    try:
        manager.switch_to_bigquery("orders")
        assert False, "expected RuntimeError"
    except RuntimeError:
        pass

    assert manager.statuses["orders"].phase == CutoverPhase.VALIDATING


def test_cutover_table_switches_on_pass(tmp_path):
    state_file = tmp_path / "state.json"
    manager = CutoverManager(state_path=str(state_file))

    status = manager.cutover_table("sessions", validate_fn=lambda _: True)

    assert status.phase == CutoverPhase.SWITCHED
    persisted = json.loads(state_file.read_text())
    assert persisted["cutover"]["sessions"]["phase"] == "switched"


def test_rollback_records_reason(tmp_path):
    manager = CutoverManager(state_path=str(tmp_path / "state.json"))
    manager.start_dual_write("orders")

    status = manager.rollback("orders", reason="checksum mismatch")

    assert status.phase == CutoverPhase.ROLLED_BACK
    assert "checksum mismatch" in status.notes[-1]


def test_generate_runbook_lists_all_tables(tmp_path):
    manager = CutoverManager(state_path=str(tmp_path / "state.json"))
    manager.cutover_table("users", validate_fn=lambda _: True)
    manager.cutover_table("orders", validate_fn=lambda _: False)

    runbook = manager.generate_runbook()

    assert "users" in runbook
    assert "orders" in runbook
