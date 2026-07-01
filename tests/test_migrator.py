import json

from src.migrator import MigrationState, load_config


def _write_config(tmp_path):
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
snowflake:
  account_env: TEST_SF_ACCOUNT
  user_env: TEST_SF_USER
  password_env: TEST_SF_PASSWORD
  database: ANALYTICS
  schemas: [PUBLIC]
bigquery:
  project_id_env: TEST_GCP_PROJECT
  dataset: migrated_dataset
  location: US
gcs:
  bucket_env: TEST_GCS_BUCKET
tables:
  - sf_table: analytics.public.users
    bq_table: users
    key_col: user_id
    watermark_col: updated_at
    wave: 1
  - sf_table: analytics.public.orders
    bq_table: orders
    key_col: order_id
    watermark_col: null
    wave: 2
"""
    )
    return config_path


def test_load_config_resolves_env_vars_and_tables(tmp_path, monkeypatch):
    config_path = _write_config(tmp_path)
    monkeypatch.setenv("TEST_SF_ACCOUNT", "acct")
    monkeypatch.setenv("TEST_SF_USER", "user")
    monkeypatch.setenv("TEST_SF_PASSWORD", "pw")
    monkeypatch.setenv("TEST_GCP_PROJECT", "proj")
    monkeypatch.setenv("TEST_GCS_BUCKET", "bucket")

    config = load_config(str(config_path))

    assert config.sf_account == "acct"
    assert config.bq_project == "proj"
    assert len(config.tables) == 2
    assert config.waves == [1, 2]
    assert config.tables_in_wave(1)[0].bq_table == "users"


def test_load_config_raises_on_missing_env_var(tmp_path, monkeypatch):
    config_path = _write_config(tmp_path)
    monkeypatch.delenv("TEST_SF_ACCOUNT", raising=False)

    try:
        load_config(str(config_path))
        assert False, "expected EnvironmentError"
    except EnvironmentError:
        pass


def test_migration_state_marks_and_checks_completion(tmp_path):
    state_path = tmp_path / "state.json"
    state = MigrationState(str(state_path))

    assert state.is_complete("transfer", "users") is False
    state.mark_complete("transfer", "users")
    assert state.is_complete("transfer", "users") is True

    persisted = json.loads(state_path.read_text())
    assert persisted["transfer"]["users"] == "completed"


def test_migration_state_loads_existing_file(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"transfer": {"orders": "completed"}}))

    state = MigrationState(str(state_path))

    assert state.is_complete("transfer", "orders") is True
