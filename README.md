# bus-eta

This repository is an exploration and visualization of the [Entur Real-Time Dataset](https://data.entur.no/domain/public-transport-data/product/realtime_siri_et/urn:li:container:1d391ef93913233c516cbadfb190dc65).
I found this data set when I was looking for something to use, in order to get to know [DuckDB](https://duckdb.org/).

It is a companion repository to a blogpost that's going to be published to [arktekk.no/blog](https://arktekk.no/blog) at some point. 
If you're reading this, maybe that's where you came from?

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
uv sync --extras=scripts
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
uv run download_entur.py
```

Run the webapp in development mode (requires extra steps, see Scripts and Dashboard app below):
```shell
uv run webapp.py
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

There's a plotly dash app that contains some visualizations in `webapp.py`. It requires `leg_stats.parquet`, 
`stop_stats.parquet` and `stop_line.parquet`. You can download them from my hetzner object storage, they're
quite small (less than 100MB total):

```shell
for f in {leg_stats,stop_stats,stop_line}.parquet; do
  curl -o $f https://kaaveland-bus-eta-data.hel1.your-objectstorage.com/$f
done
```

Run it with `uv run webapp.py` or build it with docker and run it. These files are also necessary to build
the docker image to run the app.

### Deployment

I run this using docker-compose and a nginx reverse proxy at [kollektivkart.arktekk.no](https://kollektivkart.arktekk.no).

You can use the docker image at [ghcr](https://github.com/kaaveland/bus-eta/pkgs/container/bus-eta),
it bundles the data I extracted in [EnturRealtimeEDA.ipynb](./EnturRealtimeEDA.ipynb).

Feel free to find inspiration in [docker-compose.yml](./docker-compose.yml) or [DEPLOY.md](./DEPLOY.md).

Note that `uv run webapp.py` is not a suitable way to run this application for any sort of load. Put it behind
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
- Try to refactor the webapp completely, separate out the visualizations into independent modules.
- Add automated tests.
- Set up a nightly job to fetch new data and update the app. This also requires introducing partitioning by date
  in order to avoid aggregating through all the old data again.
