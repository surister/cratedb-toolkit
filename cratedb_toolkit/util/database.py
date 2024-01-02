# Copyright (c) 2023-2024, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
import io
import os
import typing as t
from pathlib import Path

import sqlalchemy as sa
import sqlparse
from boltons.urlutils import URL
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.sql.elements import AsBoolean

from cratedb_toolkit.util.data import str_contains


def run_sql(dburi: str, sql: str, records: bool = False):
    return DatabaseAdapter(dburi=dburi).run_sql(sql=sql, records=records)


class DatabaseAdapter:
    """
    Wrap SQLAlchemy connection to database.
    """

    def __init__(self, dburi: str, echo: bool = False):
        self.dburi = dburi
        self.engine = sa.create_engine(self.dburi, echo=echo)
        self.connection = self.engine.connect()

    def run_sql(self, sql: t.Union[str, Path, io.IOBase], records: bool = False, ignore: str = None):
        """
        Run SQL statement, and return results, optionally ignoring exceptions.
        """

        sql_effective: str
        if isinstance(sql, str):
            sql_effective = sql
        elif isinstance(sql, Path):
            sql_effective = sql.read_text()
        elif isinstance(sql, io.IOBase):
            sql_effective = sql.read()
        else:
            raise TypeError("SQL statement type must be either string, Path, or IO handle")

        try:
            return self.run_sql_real(sql=sql_effective, records=records)
        except Exception as ex:
            if not ignore:
                raise
            if ignore not in str(ex):
                raise

    def run_sql_real(self, sql: str, records: bool = False):
        """
        Invoke SQL statement, and return results.
        """
        results = []
        with self.engine.connect() as connection:
            for statement in sqlparse.split(sql):
                result = connection.execute(sa.text(statement))
                data: t.Any
                if records:
                    rows = result.mappings().fetchall()
                    data = [dict(row.items()) for row in rows]
                else:
                    data = result.fetchall()
                results.append(data)

        # Backward-compatibility.
        if len(results) == 1:
            return results[0]
        else:
            return results

    def count_records(self, tablename_full: str):
        """
        Return number of records in table.
        """
        sql = f"SELECT COUNT(*) AS count FROM {tablename_full};"  # noqa: S608
        results = self.run_sql(sql=sql)
        return results[0][0]

    def table_exists(self, tablename_full: str) -> bool:
        """
        Check whether given table exists.
        """
        sql = f"SELECT 1 FROM {tablename_full} LIMIT 1;"  # noqa: S608
        try:
            self.run_sql(sql=sql)
            return True
        except Exception:
            return False

    def refresh_table(self, tablename_full: str):
        """
        Run a `REFRESH TABLE ...` command.
        """
        sql = f"REFRESH TABLE {tablename_full};"  # noqa: S608
        self.run_sql(sql=sql)
        return True

    def drop_repository(self, name: str):
        """
        Drop snapshot repository.
        """
        # TODO: DROP REPOSITORY IF EXISTS
        try:
            sql = f"DROP REPOSITORY {name};"
            self.run_sql(sql)
        except ProgrammingError as ex:
            if not str_contains(ex, "RepositoryUnknownException", "RepositoryMissingException"):
                raise

    def ensure_repository_fs(
        self,
        name: str,
        typename: str,
        location: str,
        drop: bool = False,
    ):
        """
        Make sure the repository exists, and optionally drop it upfront.
        """
        if drop:
            self.drop_repository(name)

        # TODO: CREATE REPOSITORY IF NOT EXISTS
        sql = f"""
            CREATE REPOSITORY
                {name}
            TYPE
                {typename}
            WITH (
                location   = '{location}'
            );
        """
        self.run_sql(sql)

    def ensure_repository_s3(
        self,
        name: str,
        typename: str,
        protocol: str,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        drop: bool = False,
    ):
        """
        Make sure the repository exists, and optionally drop it upfront.
        """
        if drop:
            self.drop_repository(name)

        # TODO: CREATE REPOSITORY IF NOT EXISTS
        sql = f"""
            CREATE REPOSITORY
                {name}
            TYPE
                {typename}
            WITH (
                protocol   = '{protocol}',
                endpoint   = '{endpoint}',
                access_key = '{access_key}',
                secret_key = '{secret_key}',
                bucket     = '{bucket}'
            );
        """
        self.run_sql(sql)

    def ensure_repository_az(
        self,
        name: str,
        typename: str,
        protocol: str,
        endpoint: str,
        account: str,
        key: str,
        container: str,
        drop: bool = False,
    ):
        """
        Make sure the repository exists, and optionally drop it upfront.
        """
        if drop:
            self.drop_repository(name)

        # TODO: CREATE REPOSITORY IF NOT EXISTS
        sql = f"""
            CREATE REPOSITORY
                {name}
            TYPE
                {typename}
            WITH (
                protocol   = '{protocol}',
                endpoint   = '{endpoint}',
                account    = '{account}',
                key        = '{key}',
                container  = '{container}'
            );
        """
        self.run_sql(sql)

    def import_csv_pandas(
        self, filepath: t.Union[str, Path], tablename: str, index=False, chunksize=1000, if_exists="replace"
    ):
        """
        Import CSV data using pandas.
        """
        import pandas as pd
        from crate.client.sqlalchemy.support import insert_bulk

        df = pd.read_csv(filepath)
        with self.engine.connect() as connection:
            return df.to_sql(
                tablename, connection, index=index, chunksize=chunksize, if_exists=if_exists, method=insert_bulk
            )

    def import_csv_dask(
        self,
        filepath: t.Union[str, Path],
        tablename: str,
        index=False,
        chunksize=1000,
        if_exists="replace",
        npartitions: int = None,
        progress: bool = False,
    ):
        """
        Import CSV data using Dask.
        """
        import dask.dataframe as dd
        import pandas as pd
        from crate.client.sqlalchemy.support import insert_bulk

        # Set a few defaults.
        npartitions = npartitions or os.cpu_count()

        if progress:
            from dask.diagnostics import ProgressBar

            pbar = ProgressBar()
            pbar.register()

        # Load data into database.
        df = pd.read_csv(filepath)
        ddf = dd.from_pandas(df, npartitions=npartitions)
        return ddf.to_sql(
            tablename,
            uri=self.dburi,
            index=index,
            chunksize=chunksize,
            if_exists=if_exists,
            method=insert_bulk,
            parallel=True,
        )


def sa_is_empty(thing):
    """
    When a WHERE criteria clause is empty, i.e. it contains only an
    `and_` element, let's consider it to be empty.

    TODO: Verify this. How to actually compare SQLAlchemy elements by booleanness?
    """
    return isinstance(thing, AsBoolean)


def decode_database_table(url: str) -> t.Tuple[str, str]:
    """
    Decode database and table names from database URI path and/or query string.

    Variants:

        /<database>/<table>
        ?database=<database>&table=<table>

    TODO: Synchronize with `influxio.model.decode_database_table`.
          This one uses `boltons`, the other one uses `yarl`.
    """
    url_ = URL(url)
    try:
        database, table = url_.path.strip("/").split("/")
    except ValueError as ex:
        if "too many values to unpack" not in str(ex) and "not enough values to unpack" not in str(ex):
            raise
        database = url_.query_params.get("database")
        table = url_.query_params.get("table")
        if url_.scheme == "crate" and not database:
            database = url_.query_params.get("schema")
    return database, table
