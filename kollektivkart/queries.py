import pandas as pd
from datetime import date
from duckdb.duckdb import DuckDBPyConnection


def datasources_by_name(db: DuckDBPyConnection) -> dict[str, str]:
    return {
        row[1]: row[0]
        for row
        in db.query("select dataSourceName, dataSource from datasources order by dataSourceName").fetchall()
    }


def lines_for_datasource(
    db: DuckDBPyConnection, data_source: str
) -> dict[str, str]:
    return {
        row[0].split(':')[-1]: row[0]
        for row
        in db.query(
            "select lineRef from datasource_line where dataSource = $data_source",
            params=dict(data_source=data_source))
    }


def months(
    db: DuckDBPyConnection
) -> list[date]:
    return [
        row[0]
        for row in
        db.query("select distinct month from leg_stats order by month asc").fetchall()
    ]


def legs(
    db: DuckDBPyConnection,
    month: date,
    hour: int,
    data_source: str,
    line_ref: str | None = None
) -> pd.DataFrame:
    return db.sql("""
SELECT 
  from_stop,
  to_stop,
  from_stop || ' to ' || to_stop as name,
  month,
  hour,
  air_distance_km,
  from_lat * .985 + to_lat * .015 as lat,
  from_lon * .985 + to_lon * .015 as lon,
  round(hourly_duration / monthly_duration, 1) as rush_intensity,
  round(hourly_quartile / monthly_quartile, 1) as rush_intensity_quartile,
  hourly_duration,
  hourly_quartile,
  monthly_duration,
  monthly_quartile,
  monthly_delay,
  hourly_delay,
  monthly_deviation,
  hourly_deviation,
  monthly_count,
  hourly_count  
FROM leg_stats
WHERE month = $month and hour = $hour and dataSource = $data_source
    """, params=dict(
        month=month, hour=hour, data_source=data_source
    )).df()