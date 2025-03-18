from duckdb.duckdb import DuckDBPyConnection


def create_tables(db: DuckDBPyConnection, parquet_location: str):
    db.execute(f"""
    create table leg_stats as from read_parquet('{parquet_location}/leg_stats.parquet/*/*', hive_partitioning=true);
    create table datasources as from '{parquet_location}/datasources.parquet';
    create table datasource_line as from '{parquet_location}/datasource_line.parquet';
    create table stop_line as from '{parquet_location}/stop_line.parquet';
    """)

