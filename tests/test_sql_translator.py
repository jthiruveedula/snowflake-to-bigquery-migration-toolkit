from google.api_core.exceptions import ResourceExhausted

from src.sql_translator import SnowflakeSqlTranslator, TranslationUnit


def _make_translator(mocker):
    mocker.patch(
        "src.sql_translator.migration_v2.MigrationServiceClient", return_value=mocker.Mock()
    )
    return SnowflakeSqlTranslator(project_id="proj")


def test_translate_batch_continues_past_failure(mocker):
    translator = _make_translator(mocker)
    mocker.patch.object(
        translator,
        "_translate_one",
        side_effect=[
            mocker.Mock(success=True, object_name="a"),
            mocker.Mock(success=False, object_name="b"),
        ],
    )

    units = [
        TranslationUnit(object_name="a", object_type="view", source_sql="SELECT 1"),
        TranslationUnit(object_name="b", object_type="view", source_sql="SELECT 2"),
    ]
    results = translator.translate_batch(units)

    assert len(results) == 2
    assert results[0].success is True
    assert results[1].success is False


def test_translate_one_retries_on_rate_limit_then_gives_up(mocker):
    translator = _make_translator(mocker)
    mocker.patch.object(translator.client, "create_migration_workflow", side_effect=ResourceExhausted("limited"))
    mocker.patch("time.sleep", return_value=None)

    unit = TranslationUnit(object_name="vw", object_type="view", source_sql="SELECT 1")
    result = translator._translate_one(unit)

    assert result.success is False
    assert "retries" in result.error.lower()
    assert translator.client.create_migration_workflow.call_count == 3


def test_generate_issues_report_lists_failures(mocker):
    translator = _make_translator(mocker)
    translator.results = [
        mocker.Mock(
            object_name="vw_a", object_type="view", success=True,
            untranslated_tokens=[], error=None,
        ),
        mocker.Mock(
            object_name="vw_b", object_type="view", success=False,
            untranslated_tokens=["UNSUPPORTED_FN"], error="boom",
        ),
    ]

    report = translator.generate_issues_report()

    assert "vw_a" in report
    assert "vw_b" in report
    assert "UNSUPPORTED_FN" in report
