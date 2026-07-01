from src.data_validator import MigrationValidator, ValidationResult


def _make_validator(mocker):
    mocker.patch("snowflake.connector.connect", return_value=mocker.Mock())
    mocker.patch("src.data_validator.bigquery.Client", return_value=mocker.Mock())
    return MigrationValidator(
        sf_conn_params={"account": "a", "user": "u", "password": "p"},
        bq_project="proj",
        bq_dataset="ds",
    )


def test_validate_table_uses_sampled_checksum_above_threshold(mocker):
    validator = _make_validator(mocker)
    mocker.patch.object(validator, "get_row_counts", return_value=(60_000_000, 60_000_000))
    sampled_mock = mocker.patch.object(validator, "get_sampled_checksum", return_value=("abc", "abc"))
    full_mock = mocker.patch.object(validator, "get_checksum")
    mocker.patch.object(validator, "check_column_types", return_value=[])

    result = validator.validate_table("sf.public.events", "events", "id", row_count_threshold=50_000_000)

    sampled_mock.assert_called_once()
    full_mock.assert_not_called()
    assert result.sampled is True
    assert result.passed is True


def test_validate_table_uses_full_checksum_below_threshold(mocker):
    validator = _make_validator(mocker)
    mocker.patch.object(validator, "get_row_counts", return_value=(100, 100))
    sampled_mock = mocker.patch.object(validator, "get_sampled_checksum")
    full_mock = mocker.patch.object(validator, "get_checksum", return_value=("x", "x"))
    mocker.patch.object(validator, "check_column_types", return_value=[])

    result = validator.validate_table("sf.public.users", "users", "id")

    full_mock.assert_called_once()
    sampled_mock.assert_not_called()
    assert result.sampled is False


def test_validate_table_fails_on_type_mismatch(mocker):
    validator = _make_validator(mocker)
    mocker.patch.object(validator, "get_row_counts", return_value=(100, 100))
    mocker.patch.object(validator, "get_checksum", return_value=("x", "x"))
    mocker.patch.object(
        validator, "check_column_types",
        return_value=[{"column": "new_col", "snowflake": "VARCHAR", "bigquery": "MISSING"}],
    )

    result = validator.validate_table("sf.public.users", "users", "id")

    assert result.passed is False
    assert result.type_mismatches


def test_generate_summary_dashboard_counts_pass_fail(mocker):
    validator = _make_validator(mocker)
    results = [
        ValidationResult("users", 10, 10, True, "a", "a", True, {}, True),
        ValidationResult("orders", 10, 9, False, "a", "b", False, {}, False),
    ]

    dashboard = validator.generate_summary_dashboard(results)

    assert "Passed:** 1" in dashboard
    assert "Failed:** 1" in dashboard
    assert "orders" in dashboard
