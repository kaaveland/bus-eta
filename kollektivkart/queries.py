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
  round(hourly_duration / monthly_duration, 1) as rush_intensity,
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
      round(hourly_duration / monthly_duration, 1) as rush_intensity,
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
      round(hourly_duration / monthly_duration, 1) as rush_intensity,
      hourly_duration,
      monthly_duration
    FROM leg_stats
    WHERE ($data_source IS NULL OR dataSource = $data_source) AND month = $month
    ORDER BY rush_intensity DESC
    LIMIT $limit    
    """,
        params=dict(data_source=data_source, limit=limit, month=month),
    ).df()
