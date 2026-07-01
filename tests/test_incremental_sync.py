from src.incremental_sync import IncrementalSyncer


def _make_syncer(mocker):
    mocker.patch("snowflake.connector.connect", return_value=mocker.Mock())
    mocker.patch("src.incremental_sync.bigquery.Client", return_value=mocker.Mock())
    return IncrementalSyncer(
        sf_conn_params={"account": "a", "user": "u", "password": "p"},
        bq_project="proj",
        bq_dataset="ds",
    )


def test_sync_table_prefers_stream_over_watermark(mocker):
    syncer = _make_syncer(mocker)
    mocker.patch.object(syncer, "sync_via_stream", return_value="stream-result")
    mocker.patch.object(syncer, "sync_via_watermark", return_value="watermark-result")

    result = syncer.sync_table(
        sf_table="sf.public.orders", bq_table="orders", key_col="id",
        watermark_col="updated_at", stream_name="orders_stream",
    )

    assert result == "stream-result"


def test_sync_table_falls_back_to_full_reload_without_watermark(mocker):
    syncer = _make_syncer(mocker)
    mocker.patch.object(syncer, "full_reload", return_value="reload-result")

    result = syncer.sync_table(sf_table="sf.public.sessions", bq_table="sessions", key_col="id")

    assert result == "reload-result"


def test_sync_via_watermark_no_changes_returns_zero_rows(mocker):
    syncer = _make_syncer(mocker)
    mocker.patch.object(syncer, "get_last_watermark", return_value="2024-01-01")
    mocker.patch.object(syncer, "_sf_query", return_value=[])

    result = syncer.sync_via_watermark(
        sf_table="sf.public.orders", bq_table="orders", key_col="id", watermark_col="updated_at"
    )

    assert result.rows_synced == 0
    assert result.high_watermark == "2024-01-01"


def test_sync_via_watermark_merges_changed_rows(mocker):
    syncer = _make_syncer(mocker)
    mocker.patch.object(syncer, "get_last_watermark", return_value=None)
    mocker.patch.object(
        syncer, "_sf_query",
        return_value=[{"ID": 1, "UPDATED_AT": "2024-02-01"}, {"ID": 2, "UPDATED_AT": "2024-03-01"}],
    )
    merge_mock = mocker.patch.object(syncer, "_merge_rows_into_bq")

    result = syncer.sync_via_watermark(
        sf_table="sf.public.orders", bq_table="orders", key_col="id", watermark_col="updated_at"
    )

    assert result.rows_synced == 2
    assert result.high_watermark == "2024-03-01"
    merge_mock.assert_called_once()


def test_sync_via_stream_splits_deletes_and_upserts(mocker):
    syncer = _make_syncer(mocker)
    mocker.patch.object(
        syncer, "_sf_query",
        return_value=[
            {"ID": 1, "METADATA$ACTION": "INSERT"},
            {"ID": 2, "METADATA$ACTION": "DELETE"},
        ],
    )
    merge_mock = mocker.patch.object(syncer, "_merge_rows_into_bq")
    delete_mock = mocker.patch.object(syncer, "_delete_rows_from_bq")

    result = syncer.sync_via_stream("orders_stream", "sf.public.orders", "orders", "id")

    assert result.rows_synced == 2
    merge_mock.assert_called_once()
    delete_mock.assert_called_once()
