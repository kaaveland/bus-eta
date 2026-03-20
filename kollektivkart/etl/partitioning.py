from os.path import join
from datetime import date, datetime

import duckdb
from duckdb import DuckDBPyConnection


def to_date(dt: date | datetime) -> date:
    if isinstance(dt, datetime):
        return dt.date()
    else:
        return dt


def available_daily_partitions(
    db: DuckDBPyConnection, parquet_dataset: str
) -> set[date]:
    pq = join(parquet_dataset, "*/*")
    query = (
        "select distinct operatingDate from read_parquet($pq, hive_partitioning=true)"
    )
    try:
        return {
            to_date(row[0]) for row in db.sql(query, params=(dict(pq=pq))).fetchall()
        }
    except duckdb.IOException:
        return set()


def available_monthly_partitions(
    db: DuckDBPyConnection, parquet_dataset: str, use_trunc=True
) -> set[date]:
    pq = join(parquet_dataset, "*/*")
    col = "date_trunc('month', operatingDate)" if use_trunc else "month"
    query = f"select distinct {col} from read_parquet($pq, hive_partitioning=true)"
    try:
        return {
            to_date(row[0]) for row in db.sql(query, params=(dict(pq=pq))).fetchall()
        }
    except duckdb.IOException:
        return set()
