"""Tables in PostgreSQL, for running off Databricks with DB_BACKEND=postgres.

A Unity Catalog table catalog.schema.table is schema.table in the PostgreSQL database
named after the catalog (prd_mega), the layout rpf-country-dash reads with its own
DB_BACKEND=postgres. POSTGRES_DSN names the server, the database and a role allowed to
create schemas and tables in it.

Table and schema names are lowercased, as Unity Catalog does; column names are kept as
given and always quoted, so a table reads back with the columns it was written with.

The same file is in mega-indicators and mega-boost; keep the copies identical.
"""
import os

import pandas as pd
import psycopg
from psycopg import sql

# Longer identifiers are silently truncated by PostgreSQL.
_MAX_IDENTIFIER_BYTES = 63


def _dsn():
    dsn = os.environ.get("POSTGRES_DSN")
    if not dsn:
        raise RuntimeError("DB_BACKEND=postgres requires POSTGRES_DSN, e.g. postgresql://user:password@host:5432/prd_mega")
    return dsn


def _connect(catalog):
    conn = psycopg.connect(_dsn())
    database = conn.info.dbname
    if database != catalog:
        conn.close()
        raise RuntimeError(
            f"POSTGRES_DSN points at database {database!r}; tables of catalog {catalog!r} "
            f"are kept in a database named {catalog!r}."
        )
    return conn


def _identifier(name):
    if len(name.encode()) > _MAX_IDENTIFIER_BYTES:
        raise ValueError(f"{name!r} is longer than PostgreSQL's {_MAX_IDENTIFIER_BYTES}-byte identifier limit")
    return sql.Identifier(name)


def _column_type(series):
    """The PostgreSQL type for a column, as information_schema.columns names it."""
    dtype = series.dtype
    if pd.api.types.is_bool_dtype(dtype):
        return "boolean"
    if pd.api.types.is_integer_dtype(dtype):
        return "bigint"
    if pd.api.types.is_float_dtype(dtype):
        return "double precision"
    if isinstance(dtype, pd.DatetimeTZDtype):
        return "timestamp with time zone"
    if pd.api.types.is_datetime64_dtype(dtype):
        return "timestamp without time zone"
    # object columns: booleans or numbers with nulls mixed in keep their type
    inferred = pd.api.types.infer_dtype(series, skipna=True)
    if inferred == "boolean":
        return "boolean"
    if inferred == "integer":
        return "bigint"
    if inferred in ("floating", "mixed-integer-float", "decimal"):
        return "double precision"
    return "text"


def _column_values(series, pg_type):
    values = series.astype(object)
    if pg_type == "text":
        return [None if pd.isna(v) else str(v) for v in values]
    return [None if pd.isna(v) else v for v in values]


def _existing_columns(cur, schema, table):
    cur.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
        (schema, table),
    )
    return cur.fetchall()


def table_exists(catalog, schema, table):
    """False for a table of another catalog, which this database does not hold."""
    with psycopg.connect(_dsn()) as conn, conn.cursor() as cur:
        if conn.info.dbname != catalog:
            return False
        return bool(_existing_columns(cur, schema.lower(), table.lower()))


def read_table(catalog, schema, table, columns=None):
    """schema.table as a pandas DataFrame, optionally restricted to `columns`; NULL is None or NaN."""
    selected = sql.SQL("*") if columns is None else sql.SQL(", ").join(map(_identifier, columns))
    query = sql.SQL("SELECT {} FROM {}.{}").format(selected, _identifier(schema.lower()), _identifier(table.lower()))
    with _connect(catalog) as conn, conn.cursor() as cur:
        cur.execute(query)
        return pd.DataFrame(cur.fetchall(), columns=[d.name for d in cur.description])


def replace_table(df, catalog, schema, table):
    """Replace schema.table with `df` in one transaction and return the row count.

    When the columns and their types are unchanged the table is truncated and
    refilled, so readers such as the dashboard wait for the commit instead of failing
    on a dropped table; otherwise it is dropped and recreated.
    """
    schema, table = schema.lower(), table.lower()
    columns = [str(c) for c in df.columns]
    if len(set(columns)) != len(columns):
        raise ValueError(f"duplicate column names in {schema}.{table}: {columns}")
    types = [_column_type(df[c]) for c in df.columns]
    target = sql.SQL("{}.{}").format(_identifier(schema), _identifier(table))
    column_list = sql.SQL(", ").join(map(_identifier, columns))
    rows = zip(*(_column_values(df[c], t) for c, t in zip(df.columns, types)))

    with _connect(catalog) as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(_identifier(schema)))
        if _existing_columns(cur, schema, table) == list(zip(columns, types)):
            cur.execute(sql.SQL("TRUNCATE TABLE {}").format(target))
        else:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(target))
            definitions = sql.SQL(", ").join(
                sql.SQL("{} {}").format(_identifier(c), sql.SQL(t)) for c, t in zip(columns, types)
            )
            cur.execute(sql.SQL("CREATE TABLE {} ({})").format(target, definitions))
        with cur.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(target, column_list)) as copy:
            for row in rows:
                copy.write_row(row)
    return len(df)
