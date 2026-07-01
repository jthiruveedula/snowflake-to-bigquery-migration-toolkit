from src.assessment import ObjectInventory, SnowflakeAssessor


def _make_assessor(mocker):
    mocker.patch("snowflake.connector.connect", return_value=mocker.Mock())
    return SnowflakeAssessor(sf_conn_params={"account": "a", "user": "u", "password": "p"})


def test_discover_tables_estimates_complexity(mocker):
    assessor = _make_assessor(mocker)
    mocker.patch.object(
        assessor,
        "_query",
        return_value=[
            {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "BIG", "ROW_COUNT": 2_000_000_000, "BYTES": 1},
            {"TABLE_SCHEMA": "PUBLIC", "TABLE_NAME": "SMALL", "ROW_COUNT": 10, "BYTES": 1},
        ],
    )

    objects = assessor.discover_tables("ANALYTICS")

    assert objects[0].complexity == "high"
    assert objects[1].complexity == "low"


def test_extract_dependencies_from_view_sql():
    deps = SnowflakeAssessor._extract_dependencies("SELECT * FROM users u JOIN orders o ON u.id = o.user_id")
    assert deps == ["ORDERS", "USERS"]


def test_build_wave_plan_distributes_objects(mocker):
    assessor = _make_assessor(mocker)
    assessor.inventory = [
        ObjectInventory(object_type="table", database="d", schema="s", name=f"t{i}", bytes_size=i)
        for i in range(6)
    ]

    waves = assessor.build_wave_plan(max_waves=3)

    assert sum(len(w.objects) for w in waves) == 6
    assert all(w.objects for w in waves)


def test_generate_report_includes_inventory_and_waves(mocker):
    assessor = _make_assessor(mocker)
    assessor.inventory = [ObjectInventory(object_type="table", database="d", schema="s", name="users")]
    waves = assessor.build_wave_plan(max_waves=1)

    report = assessor.generate_report(waves)

    assert "users" in report
    assert "Wave 1" in report
