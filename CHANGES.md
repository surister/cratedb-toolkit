# Changes for cratedb-toolkit


## Unreleased

- Add SQL runner utility primitives to `io.sql` namespace
- Add `import_csv_pandas` and `import_csv_dask` utility primitives
- data: Add subsystem for "loading" data.
- Add SDK and CLI for CrateDB Cloud Data Import APIs
  `ctk load table ...`
- Add `migr8` program from previous repository
- InfluxDB: Add adapter for `influxio`
- MongoDB: Add `migr8` program from previous repository
- MongoDB: Improve UX by using `ctk load table mongodb://...`
- load table: Refactor to use more OO
- Cloud API: SDK and CLI for CrateDB Cloud Cluster and Import APIs


## 2023/11/06 v0.0.2

- CLI: Upgrade to `click-aliases>=1.0.2`, fixing erroring out when no group aliases
  are specified.

- Add support for Python 3.12

- SQLAlchemy: Improve UNIQUE constraints polyfill to accept multiple
  column names, for emulating unique composite keys.


## 2023/10/10 v0.0.1

- SQLAlchemy: Add a few patches and polyfills, which do not fit well
  into the vanilla Python driver / SQLAlchemy dialect.

- Retention: Refactor strategies `delete`, `reallocate`, and `snapshot`, to
  standalone variants.

- Retention: Bundle configuration and runtime settings into `Settings` entity,
  and use more OO instead of weak dictionaries: Add `RetentionStrategy`,
  `TableAddress`, and `Settings` entities, to improve information passing
  throughout the application and the SQL templates.

- Retention: Add `--schema` option, and `CRATEDB_EXT_SCHEMA` environment variable,
  to configure the database schema used to store the retention policy
  table. The default value is `ext`.

- Retention: Use full-qualified table names everywhere.

- Retention: Fix: Compensate for `DROP REPOSITORY` now returning `RepositoryMissingException`
  when the repository does not exist. With previous versions of CrateDB, it was
  `RepositoryUnknownException`.


## 2023/06/27 v0.0.0

- Import "data retention" implementation from <https://github.com/crate/crate-airflow-tutorial>.
  Thanks, @hammerhead.
