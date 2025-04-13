import duckdb
from duckdb.duckdb import DuckDBPyConnection

def create_tables(db: DuckDBPyConnection, parquet_location: str):
    try:
        db.execute(f"create table leg_stats as from read_parquet('{parquet_location}/leg_stats.parquet/*/*', hive_partitioning=true)")
    except duckdb.duckdb.IOException:
        db.execute(f"create table leg_stats as from read_parquet('{parquet_location}/leg_stats.parquet')")

    db.execute(f"""
    create table datasources as from '{parquet_location}/datasources.parquet' join leg_stats using(dataSource) select distinct dataSource, dataSourceName;
    create table datasource_line as from '{parquet_location}/datasource_line.parquet';
    create table stop_line as from '{parquet_location}/stop_line.parquet';
    """)
