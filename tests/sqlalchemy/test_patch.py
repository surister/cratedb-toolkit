import sqlalchemy as sa

from tests.conftest import TESTDRIVE_DATA_SCHEMA


def test_inspector_vanilla(database):
    """
    Vanilla SQLAlchemy Inspector tests.
    """
    tablename = f'"{TESTDRIVE_DATA_SCHEMA}"."foobar"'
    inspector: sa.Inspector = sa.inspect(database.engine)
    database.run_sql(f"CREATE TABLE {tablename} AS SELECT 1")

    assert inspector.has_schema(TESTDRIVE_DATA_SCHEMA) is True

    table_names = inspector.get_table_names(schema=TESTDRIVE_DATA_SCHEMA)
    assert table_names == ["foobar"]

    view_names = inspector.get_view_names(schema=TESTDRIVE_DATA_SCHEMA)
    assert view_names == []

    indexes = inspector.get_indexes(tablename)
    assert indexes == []


def test_inspector_patched(database):
    """
    Patched SQLAlchemy Inspector tests.

    Both MLflow and LangChain invoke `get_table_names()` without a `schema` argument.
    This verifies that it still works, when it properly has been assigned to
    the `?schema=` connection string URL parameter.
    """
    tablename = f'"{TESTDRIVE_DATA_SCHEMA}"."foobar"'
    inspector: sa.Inspector = sa.inspect(database.engine)
    database.run_sql(f"CREATE TABLE {tablename} AS SELECT 1")
    assert inspector.has_schema(TESTDRIVE_DATA_SCHEMA) is True

    table_names = inspector.get_table_names()
    assert table_names == ["foobar"]
