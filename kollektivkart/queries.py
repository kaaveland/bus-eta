import pandas as pd
from datetime import date
from duckdb.duckdb import DuckDBPyConnection


def datasources_by_name(db: DuckDBPyConnection) -> dict[str, str]:
    return {
        row[1]: row[0]
        for row
        # TODO: Fix SOF differently
        in db.query("""
        select dataSourceName, dataSource 
        from datasources 
        where dataSource != 'SOF' 
        order by dataSourceName""").fetchall()
    }


def lines_for_datasource(db: DuckDBPyConnection, data_source: str) -> dict[str, str]:
    return {
        row[0].split(":")[-1]: row[0]
        for row in db.query(
            """select lineRef from datasource_line 
               where dataSource = $data_source 
               order by cast(regexp_extract(lineRef, '(\\d+)$') as int)""",
            params=dict(data_source=data_source),
        ).fetchall()
    }


def months(db: DuckDBPyConnection) -> list[date]:
    return [
        row[0]
        for row in db.query(
            "select distinct month from leg_stats order by month asc"
        ).fetchall()
    ]


def legs_for_download(
    db: DuckDBPyConnection,
    month: date,
    hour: int,
    data_source: str | None = None,
    line_ref: str | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    limit = f"LIMIT {limit}" if isinstance(limit, int) else ""
    join_stop = (
        "JOIN stop_line USING (dataSource, from_stop, to_stop)"
        if line_ref is not None
        else ""
    )
    where_line = (
        " AND ($line_ref is null OR $line_ref = stop_line.lineRef)"
        if line_ref is not None
        else ""
    )
    sql = f"""
SELECT 
  dataSource,
  month,  
  from_stop,
  to_stop,
  air_distance_km,
  from_lat,
  from_lon,
  to_lat,
  to_lon,
  round(hourly_quartile / monthly_duration, 1) as rush_intensity,
  hourly_duration,
  hourly_quartile,  
  monthly_duration,
  monthly_quartile,  
  monthly_delay,
  hourly_delay,
  monthly_deviation,
  hourly_deviation,
  monthly_count,
  hourly_count,  
FROM leg_stats {join_stop}
WHERE month = $month and hour = $hour and ($data_source is null or dataSource = $data_source) {where_line}
ORDER BY rush_intensity desc
{limit}
    """
    params = dict(month=month, hour=hour, data_source=data_source)
    if line_ref:
        params["line_ref"] = line_ref

    return db.sql(sql, params=params).df()


def legs(
    db: DuckDBPyConnection,
    month: date,
    hour: int,
    data_source: str,
    line_ref: str | None = None,
) -> pd.DataFrame:
    return db.sql(
        """
SELECT 
  from_stop || ' to ' || to_stop as name,
  air_distance_km :: text as air_distance_km,
  from_lat * .985 + to_lat * .015 as lat,
  from_lon * .985 + to_lon * .015 as lon,
  round(hourly_quartile / monthly_duration, 1) as rush_intensity,
  hourly_quartile,
  hourly_duration,
  monthly_duration,
  monthly_delay,
  hourly_delay,
  monthly_deviation,
  hourly_deviation,
  monthly_count,
  hourly_count  
FROM leg_stats JOIN stop_line USING (dataSource, from_stop, to_stop)
WHERE month = $month and hour = $hour and dataSource = $data_source AND ($line_ref is null OR $line_ref = stop_line.lineRef)
    """,
        params=dict(month=month, hour=hour, data_source=data_source, line_ref=line_ref),
    ).df()


def hot_spots(db: DuckDBPyConnection, month: date, hour: int, limit: int = 1000):
    return db.sql(
        """
    SELECT 
      from_stop || ' to ' || to_stop as name,
      air_distance_km :: text as air_distance_km,
      from_lat * .985 + to_lat * .015 as lat,
      from_lon * .985 + to_lon * .015 as lon,
      round(hourly_quartile / monthly_duration, 1) as rush_intensity,
      hourly_quartile,
      hourly_duration,
      monthly_duration,
      monthly_delay,
      hourly_delay,
      monthly_deviation,
      hourly_deviation,
      monthly_count,
      hourly_count  
    FROM leg_stats
    WHERE month = $month and hour = $hour
    ORDER BY rush_intensity DESC
    LIMIT $limit
        """,
        params=dict(month=month, hour=hour, limit=limit),
    ).df()


def most_rush_intensity(
    db: DuckDBPyConnection,
    month: date,
    data_source: str | None,
    limit: 100,
):
    return db.sql(
        """
    SELECT
      dataSource,
      from_stop || ' to ' || to_stop || ' between ' || hour || ':00-' || hour || ':59' as name,
      air_distance_km * 1000 :: int as air_distance_m,
      hourly_count,
      round(hourly_quartile / monthly_duration, 1) as rush_intensity,
      hourly_duration,
      hourly_quartile,
      monthly_duration
    FROM leg_stats
    WHERE ($data_source IS NULL OR dataSource = $data_source) AND month = $month
    ORDER BY rush_intensity DESC
    LIMIT $limit    
    """,
        params=dict(data_source=data_source, limit=limit, month=month),
    ).df()


def total_transports(db: DuckDBPyConnection) -> int:
    r = db.sql("select sum(hourly_count) as count from leg_stats").fetchall()
    return r[0][0]


def min_max_date(
    db: DuckDBPyConnection, parquet_location: str
) -> tuple[date, date] | None:
    parquet = f"{parquet_location}/arrivals.parquet/*/*"
    try:
        r = db.sql(
            """
            select min(operatingDate), max(operatingDate)
            from read_parquet($parquet, hive_partitioning=true) 
        """,
            params=dict(parquet=parquet),
        ).fetchall()
        return tuple(r[0])
    except Exception:
        return None


def total_arrivals(db: DuckDBPyConnection, parquet_location: str) -> int | None:
    parquet = f"{parquet_location}/arrivals.parquet/*/*"
    try:
        r = db.sql(
            """
            select count(*)
            from read_parquet($parquet, hive_partitioning=true) 
        """,
            params=dict(parquet=parquet),
        ).fetchall()
        return r[0][0]
    except Exception:
        return None


def leg_stat_count(db: DuckDBPyConnection) -> int:
    return db.sql("select count(*) from leg_stats").fetchall()[0][0]


def duckdb_memory(db: DuckDBPyConnection) -> int:
    return db.sql("select sum(memory_usage_bytes) from duckdb_memory();").fetchall()[0][
        0
    ]
