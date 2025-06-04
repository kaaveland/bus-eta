"""
Make a duckdb file from aggregated stats
"""

import os
import logging

import tempfile

import duckdb
from duckdb.duckdb import DuckDBPyConnection

_arrivals_stat = """
select
    min(operatingDate) as min_date,
    max(operatingDate) as max_date,
    count(*) as total_arrivals
from read_parquet($parquet, hive_partitioning=true)
"""
def make_tables(
    dest_db: DuckDBPyConnection,
    parquet_location: str
):
    dest_db.execute(
        f"create table leg_stats as from read_parquet('{parquet_location}/leg_stats.parquet/*/*', hive_partitioning=true) select * order by month, hour, dataSource"
    )
    dest_db.execute(f"""
    create table datasources as from '{parquet_location}/datasources.parquet' join leg_stats using(dataSource) select distinct dataSource, dataSourceName;
    create table datasource_line as from '{parquet_location}/datasource_line.parquet';
    create table stop_line as from '{parquet_location}/stop_line.parquet';
    """
    )
    parquet = os.path.join(parquet_location, "arrivals.parquet/*/*")
    dest_db.execute(f"create table arrivals_stats as {_arrivals_stat}", parameters=dict(parquet=parquet))


def run_job(root: str):
    (fd, db_f) = tempfile.mkstemp(
        suffix=".db", dir=root
    )
    os.close(fd)
    os.unlink(db_f)
    db = duckdb.connect(db_f)
    logging.info("Created new duckdb file at %s", db_f)
    try:
        make_tables(db, root)
        db.close()

        os.rename(db_f, os.path.join(root, "stats.db"))
        logging.info("Placed new duckdb file at %s", os.path.join(root, "stats.db"))
    finally:
        if os.path.exists(db_f):
            os.unlink(db_f)