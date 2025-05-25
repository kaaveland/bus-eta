"""
Run kollektivkart ETL jobs
"""

import logging
from datetime import date, timedelta
from argparse import ArgumentParser, RawDescriptionHelpFormatter

import psutil
import duckdb
from google.cloud.bigquery import Client

from . import sync, legs, leg_stats

parser = ArgumentParser(
    description=__doc__, formatter_class=RawDescriptionHelpFormatter
)

parser.add_argument(
    "--invalidate",
    action="store_true",
    help="Invalidate and recalculate all datasets except BQ",
)
parser.add_argument(
    "--max-cpus",
    default=psutil.cpu_count(logical=False),
    help="Limit the number of CPU cores used",
    type=int,
)
parser.add_argument(
    "--memory-limit-gb",
    default=int(0.8 * psutil.virtual_memory().available / 1e9),
    help="GB of memory to allow DuckDB (default 80%% of available)",
    type=int,
)
parser.add_argument(
    "--skip-bq", action="store_true", help="Do not fetch new data in BigQuery"
)
parser.add_argument(
    "data", help="Data repository, a folder or s3:// prefix to place output", type=str
)

_setup = """
set threads = {threads};
set memory_limit = '{mem_limit}GB';
install spatial;
load spatial;
"""


def main():
    logging.basicConfig(level=logging.INFO)
    opts = parser.parse_args()
    db = duckdb.connect(":memory:")
    setup = _setup.format(threads=opts.max_cpus, mem_limit=opts.memory_limit_gb)
    db.execute(setup)
    root = opts.data
    logging.info(
        "Allow resource usage: cpu=%s ram=%sGB", opts.max_cpus, opts.memory_limit_gb
    )
    if not opts.skip_bq:
        client = Client()
        sync.run_job(
            client,
            db,
            root,
            from_date=date(2024, 1, 1),
            to_date=date.today() - timedelta(days=1),
        )
    if opts.invalidate:
        logging.info("Invalidate downstream of BQ")
    legs.run_job(db, root, opts.invalidate)
    leg_stats.run_job(db, root, opts.invalidate)


if __name__ == "__main__":
    main()
