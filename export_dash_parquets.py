#!/usr/bin/env python
"""
Export aggregated tables from DuckDb.

Writes three parquet files:

- `leg_stats.parquet` has aggregations broken down by previous stop, stop
- `stop_stats.parquet` has aggregations broken down by stop -- all legs that end at `stop`, together
- `line_stop.parquet` maps which `lineRef`s that have been seen at which previous stop, stop for a year, month, hour
"""

import argparse

import duckdb
import psutil

parser = argparse.ArgumentParser(
    description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument(
    "duckdb_file",
    help="Path to database file containing `arrivals` and `quays` tables"
)
parser.add_argument(
    "--max_cpus", default=psutil.cpu_count(logical=False) - 1,
    help="Limit the number of cores used", type=int
)
parser.add_argument(
    "--memory-limit-gb",
    default=int(.8 * psutil.virtual_memory().available / 1e9),
    help="GB of memory to allow DuckDB to use (default 80% of available)",
    type=int
)

opts = parser.parse_args()
db = duckdb.connect(opts.duckdb_file)
db.execute(f"set threads = {opts.max_cpus}")
db.execute(f"set memory_limit = '{opts.memory_limit_gb}GB';")
db.execute("copy (select * from stop_stats order by year, month, hour, stop) to 'stop_stats.parquet';")
db.execute("copy (select * from leg_stats order by year, month, hour, stop) to 'leg_stats.parquet';")
db.execute("copy (select * from stop_line order by year, month, hour, stop) to 'stop_line.parquet';")