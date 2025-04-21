import os

from . import queries
from dash import html
from duckdb.duckdb import DuckDBPyConnection


def create(db: DuckDBPyConnection) -> html.Div:
    goal = html.P(
        children=[
            "This page is a tool for exploring delays and deviations in public transit in Norway over time and place. ",
            "It was created by using ",
            html.A(href="https://data.entur.no", children="data collected by Entur"),
            " and analyzing it with ",
            html.A(href="https://duckdb.org", children="DuckDB."),
            " The initial release had a companion ",
            html.A(
                href="https://arktekk.no/blogs/2025_entur_realtimedataset",
                children="blogpost",
            ),
            " explaining some of the motivation and methods. ",
            "The page focuses on analysing legs, the travel between two subsequent stop places in a public transit schedule."
            " It aims to look into where and when the transit takes longer than usual.",
        ]
    )
    license = html.P(
        children=[
            "The code is available under the MIT license at ",
            html.A(href="https://github.com/kaaveland/bus-eta", children="GitHub"),
            " and the data is available under the ",
            html.A(href="https://data.norge.no/nlod/no/1.0", children="NLOD license."),
        ]
    )
    parquet_location = os.environ.get("PARQUET_LOCATION", "data")
    dates = queries.min_max_date(db, parquet_location)
    total_arrivals = queries.total_arrivals(db, parquet_location)
    count = queries.total_transports(db)
    legs_count = f"The currently loaded data set contains {count:,} legs. "
    leg_stat_count = queries.leg_stat_count(db)
    memory_requirement = queries.duckdb_memory(db) / 1e9
    aggregation = (
        f"The data was aggregated to {leg_stat_count:,} rows of statistics for visualization purposes and occupies "
        f"{memory_requirement:.3f}GB of RAM in memory right now."
    )
    dataset_size = (
        html.P(
            children=[
                legs_count,
                f"It was created from {total_arrivals:,} arrival registrations between  {dates[0]} and {dates[1]}. ",
                aggregation,
            ]
        )
        if dates and total_arrivals
        else html.P(children=[legs_count, aggregation])
    )

    return html.Div(children=[html.H2("About this page"), goal, license, dataset_size])
