# bus-eta

This repository is an exploration and visualization of the [Entur Real-Time Dataset](https://data.entur.no/domain/public-transport-data/product/realtime_siri_et/urn:li:container:1d391ef93913233c516cbadfb190dc65).
I found this data set when I was looking for something to use, in order to get to know [DuckDB](https://duckdb.org/).

It is a companion repository to a blogpost that's going to be published to [arktekk.no/blog](https://arktekk.no/blog) at some point. If you're reading this, maybe
that's where you came from?

I meant for this to just be a quick and fun session to kill some time one evening, but it escalated a little bit, because it was _very_ fun.

## Setup

This repository uses `uv`, you can get it from [here](https://docs.astral.sh/uv/). It will create a virtualenv in `.venv`, which
is good to know if you want to configure an editor or IDE to find the appropriate sources.

Short note on dependencies: For friendly setup, `pyproject.toml` is set up with a huge set of dependencies. If wanting 
to use this for anything professional, I would recommend setting up 2 or 3 projects with different sets of dependencies.

Install dependencies:

```shell
uv sync
```

Add a dependency:

```shell
uv add plotly
```

Run jupyter:

```shell
uv run jupyter
```

Run a script:

```shell
uv run download_entur.py
```

## License

MIT -- see [LICENSE.md](LICENSE.md). You can use this code for any purpose, and you do not have to attribute it to me.

## Data license

All data that is fetched and used by this repository is owned by Entur, see [data.entur.no](https://data.entur.no/domain/public-transport-data).
It is available under the [NLOD](https://data.norge.no/nlod/no/1.0) license.

## What's here


### Notebooks

[EnturRealtimeEDA.ipynb](./EnturRealtimeEDA.ipynb) is an analysis I did on the real time data set to get familiar with 
it. This will consume a lot of memory with PyCharm or the IntelliJ notebook-plugin due to its size, I had to increase
heap size to 4096m.

This notebook produces a few files:

- `arrivals.parquet` which contains "raw" data from the BigQuery table
- `entur.db` which is a DuckDB instance with several tables, including `arrivals`
- `leg_stats.parquet` contains aggregated statistics for public transit stop-to-stop legs
- `stop_stats.parquet` contains aggregated statistics for public transit legs that _arrived at each stop_.
 
It resulted in a set of scripts that can be used to work with different slices of data sets:

### Scripts

- `download_entur.py` can be easily adapted to produce a DuckDB file with data from the BigQuery tables, it pages
  through a BigQuery table with the query that I used to get my raw data. I extracted this code from the notebook because
  I thought someone else might want to use it, and it isn't convenient to run notebooks for this purpose. It will create
  the `arrivals` table and the `quays` (geolocations of `stopPointRef`) table in DuckDB, see `--help` for more.
- `calculate_legs.py` can generate transit legs from real time raw data to DuckDB. It assumes the existence of the `arrivals`
  and `quays` tables and writes to the `legs` table.
- `aggregate_legs.py` can aggregate `leg_stats.parquet` and `stop_stats.parquet` from transit legs in DuckDB. It assumes
  the existence of the `legs` table.
- `export_dash_parquets.py` exports parquet files for consumption by the dash app and convenient data sharing in object storage.

### Dashboard app

There's a plotly dash app that contains some visualizations in `webapp.py`. It requires `leg_stats.parquet`
and `stop_stats.parquet`.

Run it with `uv run webapp.py` or build it with docker and run it.

### Deployment

I run this using docker-compose and a nginx reverse proxy at TODO: insert a link to deployment.

You can use the docker image at TODO: set up ghcr.io/kaaveland/bus-eta. Feel free to copy my TODO: docker-compose.yml.

Note that `uv run webapp.py` is not a suitable way to run this application for any sort of load. Put it behind
[gunicorn](https://gunicorn.org/) or something else suitable. The docker image takes care of this already.

NB! DuckDB is threaded on the C-level and puts all the data in-memory of the running process. If you want to run
it with `gunicorn` in forking mode (`--workers > 1`), you should limit the number of threads that DuckDB can use,
and make sure to use copy-on-write memory, or you'll get some really annoying problems.

## Contributions & tickets

You're welcome to file tickets & issues. You're welcome to contribute patches. Just be aware that this was a
hobby/passion project with a clear goal (learn DuckDB), so unless there's significant interest in making it
into something more than a proof of concept, it is likely that I will stop working on it.