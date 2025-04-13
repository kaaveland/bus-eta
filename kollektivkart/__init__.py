import os

import flask
import dash
import duckdb
from flask import g
from dash import html, dcc, Output, Input
import plotly.express as px

from . import initdb
from . import queries
from . import global_inputs
from . import mapview

db = duckdb.connect()
initdb.create_tables(db, os.environ.get("PARQUET_LOCATION", "data"))
server = flask.Flask(__name__)


@server.before_request
def connect_db():
    g.db = db.cursor()


@server.after_request
def close_db(r):
    g.db.close()
    return r


external_scripts = (
    [{"src": "https://scripts.simpleanalyticscdn.com/latest.js", "async": ""}]
    if "SIMPLE_ANALYTICS" in os.environ
    else []
)

app = dash.Dash(
    name="kollektivkart",
    title="Public transit study",
    server=server,
    external_scripts=external_scripts,
)
state = dcc.Store(id="state", data=dict(zoom=8, center=dict(lat=59.91, lon=10.79)))

rush_intensity = html.P(
    "Rush intensity is a measure of how much slower traffic flows between 2 stops during one particular "
    "hour, compared to the rest of the day, measured for a whole month. A rush intensity of 2 between "
    "8:00 and 8:59 means traffic takes twice as much time as what is normal for the rest of the day. "
)

app.layout = html.Div(
    [
        html.H1("Public transit study"),
        global_inputs.render_global_inputs(db),
        state,
        dcc.Tabs(
            id="tabs",
            value="map-tab",
            children=[
                dcc.Tab(
                    label="Map",
                    value="map-tab",
                    children=[
                        html.H2("View all legs for one region"),
                        html.Label(
                            "Choose line to filter map (start typing for suggestions)",
                            htmlFor="line-picker",
                        ),
                        dcc.Dropdown(id="line-picker"),
                        dcc.Graph(id="main-map"),
                    ],
                ),
                dcc.Tab(
                    label="Hot Spots",
                    value="hot-spots",
                    children=[
                        html.H2(
                            "View the 1000 most rush-affected legs for all regions"
                        ),
                        html.P(
                            "This map shows which legs in the data set that had the most rush intensity for the selected "
                            "hour and month, across all data sources (regions)."
                        ),
                        rush_intensity,
                        dcc.Graph(id="hotspot-map"),
                    ],
                ),
                dcc.Tab(
                    label="Rush intensity",
                    value="rush-intensity",
                    children=[
                        html.H2("Most rush intensity detected in data set"),
                        html.P(
                            "This visualization shows which legs in the data set that had the most rush-intensity overall."
                        ),
                        rush_intensity,
                        dcc.Graph(id="rush-intensity"),
                    ],
                ),
            ],
        ),
    ]
)


@app.callback(Output("line-picker", "options"), Input("datasource", "value"))
def set_lines_for_data_source(data_source: str):
    return {
        line: line for line in queries.lines_for_datasource(g.db, data_source).values()
    }


mapview.main_map_view(app, state)
mapview.hot_spots(app, state)


@app.callback(Output("rush-intensity", "figure"), Input("datasource", "value"), Input("month", "value"))
def worst_rush_intensity(data_source: str, month: int):
    months = queries.months(g.db)
    month = months[month]
    df = queries.most_rush_intensity(g.db, month, data_source, limit=50)
    return px.bar(
        df.sort_values(by="rush_intensity"),
        x="rush_intensity",
        y="name",
        orientation="h",
        height=800,
        color="hourly_count",
        hover_data=["air_distance_m", "hourly_duration", "monthly_duration"],
    )


__all__ = [initdb, queries, app, server]

if __name__ == "__main__":
    app.run_server(debug=True)
