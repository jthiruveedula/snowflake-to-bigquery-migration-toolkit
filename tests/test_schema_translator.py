from src.schema_translator import SnowflakeToBigQueryTranslator


def _make_translator(mocker, with_sf_conn=False):
    mocker.patch("src.schema_translator.bigquery.Client", return_value=mocker.Mock())
    mocker.patch("src.schema_translator.SnowflakeSqlTranslator", return_value=mocker.Mock())
    sf_conn_params = {"account": "a", "user": "u", "password": "p"} if with_sf_conn else None
    return SnowflakeToBigQueryTranslator(
        project_id="proj", dataset_id="ds", sf_conn_params=sf_conn_params
    )


def test_translate_type_number_without_scale_is_int64(mocker):
    translator = _make_translator(mocker)
    bq_type, precision, scale = translator.translate_type("NUMBER(38,0)")
    assert bq_type == "INT64"
    assert precision == 38


def test_translate_type_unknown_defaults_to_string_and_logs_warning(mocker):
    translator = _make_translator(mocker)
    bq_type, _, _ = translator.translate_type("FROBNICATE")
    assert bq_type == "STRING"
    assert translator.translation_errors


def test_fetch_snowflake_columns_requires_conn_params(mocker):
    translator = _make_translator(mocker, with_sf_conn=False)
    try:
        translator.fetch_snowflake_columns("db.schema.table")
        assert False, "expected RuntimeError"
    except RuntimeError:
        pass


def test_fetch_snowflake_columns_queries_information_schema(mocker):
    translator = _make_translator(mocker, with_sf_conn=True)
    fake_conn = mocker.Mock()
    fake_cursor = mocker.Mock()
    fake_cursor.fetchall.return_value = [{"column_name": "id", "data_type": "NUMBER"}]
    fake_conn.cursor.return_value = fake_cursor
    mocker.patch("snowflake.connector.connect", return_value=fake_conn)

    columns = translator.fetch_snowflake_columns("db.schema.users")

    assert columns == [{"column_name": "id", "data_type": "NUMBER"}]
    fake_cursor.execute.assert_called_once()


def test_detect_clustering_parses_cluster_by_ddl(mocker):
    translator = _make_translator(mocker, with_sf_conn=True)
    fake_conn = mocker.Mock()
    fake_cursor = mocker.Mock()
    fake_cursor.fetchall.return_value = [{"cluster_by": "LINEAR(COL_A, COL_B)"}]
    fake_conn.cursor.return_value = fake_cursor
    mocker.patch("snowflake.connector.connect", return_value=fake_conn)

    cluster_cols = translator.detect_clustering("db.schema.users")

    assert cluster_cols == ["col_a", "col_b"]


def test_detect_clustering_returns_empty_when_no_table_found(mocker):
    translator = _make_translator(mocker, with_sf_conn=True)
    fake_conn = mocker.Mock()
    fake_cursor = mocker.Mock()
    fake_cursor.fetchall.return_value = []
    fake_conn.cursor.return_value = fake_cursor
    mocker.patch("snowflake.connector.connect", return_value=fake_conn)

    assert translator.detect_clustering("db.schema.users") == []
