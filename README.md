# bus-eta

This repository is an exploration and visualization of the [Entur Real-Time Dataset](https://data.entur.no/domain/public-transport-data/product/realtime_siri_et/urn:li:container:1d391ef93913233c516cbadfb190dc65).
I found this data set when I was looking for something to use, in order to get to know [DuckDB](https://duckdb.org/).

It is a companion repository to a blogpost that's going to be published to [arktekk.no/blog](https://arktekk.no/blogs/2025_entur_realtimedataset)
at some point. If you're reading this, maybe that's where you came from?

I meant for this to just be a quick and fun session to kill some time one evening, but it escalated a little bit,
because it was _very_ fun. Overall, I spent every free evening I had for some 10-12 days, working on this. I'm
thankful to have such an understanding family. It is definitely code on the prototype-stage, though. There are
tons and tons of usability/UX problems.

## Setup

This repository uses `uv`, you can get it from [here](https://docs.astral.sh/uv/). It will create a virtualenv in `.venv`, which
is good to know if you want to configure an editor or IDE to find the appropriate sources.

Short note on dependencies: For friendly setup, `pyproject.toml` is set up with a huge set of dependencies. If wanting 
to use this for anything professional, I would recommend setting up 2 or 3 projects with different sets of dependencies.

Install dependencies, including jupyter (required for IDE-integration with notebooks):

```shell
uv sync --all-extras
```

Install all dependencies for the scripts, but skip jupyter:

```shell
uv sync --extra=scripts
```

Install dependencies for the webapp only (also requires data files, see below):
```shell
uv sync
```

Add a dependency:

```shell
uv add plotly
```

Run jupyter:

```shell
uv run --with jupyter jupyter
```

Run a script:

```shell
uv run scripts/sync_parquets.py -h
```

Run the webapp in development mode (requires extra steps, see Scripts and Dashboard app below):
```shell
uv run python -m kollektivkart
```

## License

MIT -- see [LICENSE.md](LICENSE.md). You can use this code for any purpose, and you do not have to attribute it to me.

You also do not get to blame me if something in this repository becomes a black hole that 
consumes absolutely all of your RAM.

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
 
### Scripts

See the `scripts/` folder for scripts used to download and transform data.

- `sync_parquets.py` -- downloads the `stops`, `quays` and `arrivals` (real time registrations) datasets as parquet files from a specified date: `uv run scripts/sync_parquets.py -h`
- `calculate_parquet_legs.py` -- relies on the output from `sync_parquets.py` to produce `legs.parquet`, which matches each arrival at a stop, by the arrival from the same vehicle at the previous stop. See `uv run scripts/calculate_parquet_legs.py -h`.
- `aggregations_for_app.py` -- produces all the parquet files necessary for the dashboard app to start by aggregating legs.

### Dashboard app

There's a plotly dash app that contains some visualizations in `kollektivkart/`. It requires some data files, see `kollektivkart/initdb.py`.

Samples of these can be found in my public object storage. You can run this to get some data to work with locally:

```shell
for f in {datasources,datasource_line,stop_line,leg_stats}.parquet; do
  curl -o data/$f https://kaaveland-bus-eta-data.hel1.your-objectstorage.com/devdata/$f
done
```

Run it with `uv run python -m kollektivkart` or build it with docker and run it. The dash webapp needs these files at 
runtime to work. If running with docker, use a volume and provide the `PARQUET_LOCATION` environment location to their 
location. If you want to load them directly from S3, you can provide an environment file like so:

```shell
AWS_REGION=hel1.your-objectstorage.com
DUCKDB_S3_ENDPOINT=hel1.your-objectstorage.com
AWS_ACCESS_KEY_ID=YOUR_OWN_AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_OWN_AWS_SECRET_ACCESS_KEY
PARQUET_LOCATION=s3://kaaveland-bus-eta-data
```

### Deployment

I run this using podman and a nginx reverse proxy at [kollektivkart.arktekk.no](https://kollektivkart.arktekk.no).

You can use the docker image at [ghcr](https://github.com/kaaveland/bus-eta/pkgs/container/bus-eta), as discussed in the Dashboard app section, it requires access to data files.

Feel free to find inspiration in [DEPLOY.md](./DEPLOY.md).

Note that `uv run python -m kollektivkart` is not a suitable way to run this application for any sort of load. Put it behind
[gunicorn](https://gunicorn.org/) or something else suitable. The docker image takes care of this already.

NB! This webapp puts a lot of data (> 500MB) in memory once it loads, so use `--preload` with `gunicorn`.
This ensures faster startup and also since this memory is only written once, it can be shared with copy-on-write
memory between the workers. This lets you run many workers without using a lot of RAM. gunicorn usually
recommends 1-2 workers per CPU, but since DuckDB is also threaded on the C level, 2 workers per CPU may be a 
little high.

## Contributions & tickets

You're welcome to file tickets & issues. You're welcome to contribute patches. Just be aware that this was a
hobby/passion project with a clear goal (learn DuckDB), so unless there's significant interest in making it
into something more than a proof of concept, it is likely that I will stop working on it.

## Where to next?

As alluded to under the previous section, I've not decided whether to make something more of this yet. If I did,
here are some things that _should_ be done:

- Tons of usability bugs and annoyances to fix in the webapp.
- Fix structural issues in the project, separate the scripts, notebooks and the webapp into different packages.
- Add automated tests.
- Set up a nightly job to fetch new data and update the app. This also requires introducing partitioning by date
  in order to avoid aggregating through all the old data again.
