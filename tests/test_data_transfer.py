from src.data_transfer import DataTransferOrchestrator


def _make_orchestrator(mocker):
    mocker.patch("snowflake.connector.connect", return_value=mocker.Mock())
    mocker.patch("src.data_transfer.storage.Client", return_value=mocker.Mock())
    mocker.patch("src.data_transfer.bigquery.Client", return_value=mocker.Mock())
    mocker.patch(
        "src.data_transfer.datatransfer.DataTransferServiceClient", return_value=mocker.Mock()
    )
    return DataTransferOrchestrator(
        sf_conn_params={"account": "a", "user": "u", "password": "p"},
        gcp_project="proj",
        gcs_bucket="bucket",
        bq_dataset="ds",
    )


def test_backfill_table_succeeds_first_try(mocker):
    orchestrator = _make_orchestrator(mocker)
    mocker.patch.object(orchestrator, "export_table_to_gcs", return_value="gcs://bucket/path/")
    mocker.patch.object(orchestrator, "load_gcs_to_bq", return_value=1000)

    progress = orchestrator.backfill_table("sf.public.users", "users")

    assert progress.status == "completed"
    assert progress.rows_transferred == 1000
    assert progress.attempts == 1


def test_backfill_table_retries_then_fails(mocker):
    orchestrator = _make_orchestrator(mocker)
    mocker.patch.object(orchestrator, "export_table_to_gcs", side_effect=RuntimeError("network blip"))
    mocker.patch("time.sleep", return_value=None)

    progress = orchestrator.backfill_table("sf.public.users", "users")

    assert progress.status == "failed"
    assert progress.attempts == 3
    assert "network blip" in progress.error


def test_backfill_all_continues_past_per_table_failure(mocker):
    orchestrator = _make_orchestrator(mocker)
    mocker.patch.object(
        orchestrator,
        "backfill_table",
        side_effect=[
            mocker.Mock(table_name="users", status="completed"),
            mocker.Mock(table_name="orders", status="failed"),
        ],
    )

    results = orchestrator.backfill_all(
        [{"sf_table": "sf.public.users", "bq_table": "users"},
         {"sf_table": "sf.public.orders", "bq_table": "orders"}]
    )

    assert len(results) == 2
    assert results[0].status == "completed"
    assert results[1].status == "failed"


def test_progress_report_renders_table_rows(mocker):
    orchestrator = _make_orchestrator(mocker)
    mocker.patch.object(orchestrator, "export_table_to_gcs", return_value="gcs://bucket/path/")
    mocker.patch.object(orchestrator, "load_gcs_to_bq", return_value=42)
    orchestrator.backfill_table("sf.public.users", "users")

    report = orchestrator.progress_report()

    assert "users" in report
    assert "42" in report
