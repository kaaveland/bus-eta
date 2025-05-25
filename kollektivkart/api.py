import os
import sys
from functools import cache
from datetime import date, datetime, timedelta

import orjson
import pandas as pd
from pandas.api.types import is_numeric_dtype
from flask import g, Response, request, jsonify
from flask.blueprints import Blueprint

from . import queries

app = Blueprint("api", __name__)


@app.after_request
def set_headers(response: Response) -> Response:
    response.headers.add("Access-Control-Allow-Origin", "*")
    response.headers.add("Access-Control-Allow-Methods", "GET,OPTIONS")
    response.headers["Cache-Control"] = "public, max-age=3600"
    response.headers["Expires"] = (datetime.now() + timedelta(seconds=3600)).strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )
    response.headers.add("Vary", "line_ref")
    response.headers.add("Vary", "data_source")
    return response


def to_json(df: pd.DataFrame) -> Response:
    resp = orjson.dumps(
        {
            column: df[column].to_numpy()
            if is_numeric_dtype(df[column])
            else df[column].tolist()
            for column in df.columns
        },
        option=orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_NAIVE_UTC,
    )
    return Response(resp, content_type="application/json")


@app.route("/hot-spots/<int:year>/<int:month>/<int:hour>")
def hot_spots(year: int, month: int, hour: int) -> Response:
    partition = date(year, month, 1)
    data = queries.hot_spots(g.db, partition, hour, limit=1000)
    return to_json(data)


@app.route("/leg-stats/<int:year>/<int:month>/<int:hour>/<datasource>")
def leg_stats(year: int, month: int, hour: int, datasource: str) -> Response:
    partition = date(year, month, 1)
    line_ref = request.args.get("line_ref")
    data = queries.legs(g.db, partition, hour, datasource, line_ref)
    return to_json(data)


@app.route(
    "/comparison/<int:cur_year>/<int:cur_month>/<int:prev_year>/<int:prev_month>/<int:hour>"
)
def comparison(
    cur_year: int, cur_month: int, prev_year: int, prev_month: int, hour: int
) -> Response:
    data_source = request.args.get("data_source")
    line_ref = request.args.get("line_ref")
    cur = date(cur_year, cur_month, 1)
    prev = date(prev_year, prev_month, 1)
    data = queries.comparisons(g.db, cur, prev, hour, data_source=data_source, line_ref=line_ref, limit=2000)
    return to_json(data)


@app.route("/datasource-names")
def datasources() -> Response:
    return jsonify(queries.datasources_by_name(g.db))


@app.route("/partitions")
def partitions() -> Response:
    return jsonify([{"year": d.year, "month": d.month} for d in queries.months(g.db)])


def label_key(label: str) -> tuple[int, str]:
    try:
        return int(label.split("_")[-1]), label
    except ValueError:
        return sys.maxsize, label


@app.route("/lines/<datasource>")
def lines(datasource: str) -> Response:
    data = (
        {"label": label, "line_ref": ref}
        for label, ref in queries.lines_for_datasource(g.db, datasource).items()
    )
    return jsonify(sorted(data, key=lambda item: label_key(item["label"])))


@cache
def get_stats() -> dict[str, object]:
    pq = os.environ.get("PARQUET_LOCATION", "data")
    start, end = queries.min_max_date(g.db, pq)
    return dict(
        memory=queries.duckdb_memory(g.db),
        leg_count=queries.total_transports(g.db),
        arrivals_count=queries.total_arrivals(g.db, pq),
        date_range=dict(start=start.isoformat(), end=end.isoformat()),
        aggregated_count=queries.leg_stat_count(g.db),
    )


@app.route("/stats")
def stats() -> Response:
    return jsonify(get_stats())


@app.route("/rush-intensity-rank/<int:year>/<int:month>")
def rush_intensity_rank(year: int, month: int) -> Response:
    partition = date(year, month, 1)
    return to_json(
        queries.most_rush_intensity(
            g.db, partition, request.args.get("data_source"), limit=100
        )
    )

@app.route("/ready")
def readycheck():
    latest_data = date.fromisoformat(get_stats()["date_range"]["end"])
    stale = latest_data < date.today() - timedelta(days=3)
    response = dict(status="up", data_date=latest_data.isoformat(), stale=stale)
    return Response(
        orjson.dumps(response),
        status=500 if stale else 200,
        content_type="application/json",
    )
