#!/usr/bin/env python
"""
Create a DuckDB database containing the Entur realtime dataset and quays. The database will
contain 2 tables: `arrivals` with realtime data and `quays` with geolocation of the `stopPointRef`s
referred to by `arrivals`.

This requires a `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
"""

import sys
import math
import argparse

import duckdb
import tqdm
from google.cloud import bigquery
from datetime import timedelta, date

parser = argparse.ArgumentParser(
    description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
)
parser.add_argument(
    "output",
    help="Path to use for creating DuckDB database",
)
parser.add_argument(
    "--start-date",
    default="2025-01-01",
    help="The earliest operatingDate to fetch in YYYY-MM-DD format",
    type=date.fromisoformat,
)
parser.add_argument(
    "--data-source",
    default=["BNR", "ATB", "FLT", "SJN"],
    help="The dataSources to filter select for",
    nargs="+",
)
parser.add_argument(
    "--page-days", default=30, type=int, help="Number of days for each page size"
)
parser.add_argument(
    "--auto-accept", default=False, help="Disable preview, just run the queries"
)
opts = parser.parse_args()
start, end = opts.start_date, date.today() + timedelta(days=1)

client = bigquery.Client()
db = duckdb.connect(opts.output)
data_sources = ", ".join(f"'{ds}'" for ds in opts.data_source)
data_source_filter = f"dataSource IN ({data_sources})"
source_table = (
    "ent-data-sharing-ext-prd.realtime_siri_et.realtime_siri_et_last_recorded"
)

query_template = f"""
SELECT 
  recordedAtTime,
  lineRef,
  directionRef,
  operatingDate,
  vehicleMode,
  extraJourney,
  journeyCancellation,
  stopPointRef,
  sequenceNr,
  stopPointName,
  originName,
  destinationName,
  extraCall,
  stopCancellation,
  estimated,
  aimedArrivalTime,
  arrivalTime,
  aimedDepartureTime,
  departureTime,
  datedServiceJourneyId
FROM `{source_table}`
WHERE {data_source_filter}
"""

page_delta = timedelta(days=opts.page_days)
page_count = math.ceil(((end - start) / page_delta))
page_boundaries = [
    start + n * page_delta
    for n in range(page_count + 1)  # include last page
]

if not opts.auto_accept:
    proceed = input(
        f"Will run from {start.isoformat()} - {end.isoformat()}:"
        f"{query_template} AND operatingDate >= :page_start AND operatingDate < :page_end;\n"
        "Accept? [Y/n]"
    ).strip()

    if not proceed.lower() in ("", "y"):
        print("Canceled.")
        sys.exit(1)


created = False
for page_start, page_end in tqdm.tqdm(list(zip(page_boundaries, page_boundaries[1:]))):
    job = client.query(
        f"{query_template} AND operatingDate >= '{page_start.isoformat()}' AND operatingDate < '{page_end.isoformat()}'"
    )
    tab = job.to_arrow()
    if created:
        db.execute("insert into arrivals select * from tab")
    else:
        db.execute("create table arrivals as select * from tab")
        created = True

quays_table = "ent-data-sharing-ext-prd.national_stop_registry.quays_last_version"

quays = (
    client.query(
        f"select id, location_longitude, location_latitude from `{quays_table}`"
    )
    .result()
    .to_arrow()
)

db.execute("create table quays as select * from quays")
stats = db.sql("""select
(select count(*) from arrivals) as arrivals_count,
(select count(*) from quays) as quays_count
""").df()
print(stats)
