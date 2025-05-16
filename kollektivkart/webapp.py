import os
import io

import flask
import dash
import duckdb
from flask import g, send_file, request, jsonify
from dash import html, dcc, Output, Input
import plotly.express as px

from . import about
from . import initdb
from . import queries
from . import global_inputs
from . import mapview
from . import api

db = duckdb.connect()
initdb.create_tables(db, os.environ.get("PARQUET_LOCATION", "data"))
server = flask.Flask(__name__)
server.register_blueprint(
    api.app, url_prefix="/api"
)

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
hot_spots_state = dcc.Store(
    id="hot-spot-state", data=dict(zoom=5, center=dict(lat=59.91, lon=10.79))
)

rush_intensity = html.P(
    "Rush intensity is a measure of how much slower traffic flows between 2 stops during one particular "
    "hour, compared to the rest of the day, measured for a whole month. A rush intensity of 2 between "
    "8:00 and 9:00 means traffic takes twice as much time as what is normal for the rest of the day. The "
    "rush intensity is calculated by finding the 75% percentile travel time during the chosen hour and "
    "dividing it by the 50% percentile travel time during the whole month."
)

app.layout = html.Div(
    [
        html.H1("Public transit study"),
        global_inputs.render_global_inputs(db),
        state,
        hot_spots_state,
        dcc.Tabs(
            id="tabs",
            value="hot-spots",
            children=[
                dcc.Tab(
                    label="Hot Spots",
                    value="hot-spots",
                    children=[
                        html.H2(
                            "View the 1000 most rush-affected legs for all regions"
                        ),
                        dcc.Graph(id="hotspot-map"),
                        html.P(
                            "This map shows which legs in the data set that had the most rush intensity for the selected "
                            "hour and month, across all data sources (regions)."
                        ),
                        rush_intensity,
                        html.Div(id="download-hot-spot-csv"),
                    ],
                ),
                dcc.Tab(
                    label="Region Map",
                    value="map-tab",
                    children=[
                        html.H2("View all legs for one region"),
                        html.Label("Select data source", htmlFor="datasource"),
                        dcc.Dropdown(
                            id="datasource",
                            #value="RUT",
                            options=queries.datasources_by_name(db),
                        ),
                        html.Label(
                            "Choose line to filter map (start typing for suggestions)",
                            htmlFor="line-picker",
                        ),
                        dcc.Dropdown(id="line-picker"),
                        dcc.Graph(id="main-map"),
                        html.Div(id="download-region-csv"),
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
                dcc.Tab(label="About", value="about", children=about.create(db)),
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
mapview.hot_spots(app, hot_spots_state)


@app.callback(
    Output("download-hot-spot-csv", "children"),
    Input("month", "value"),
    Input("hour", "value"),
)
def set_download_hotspot_link(month, hour):
    return [
        dcc.Link(
            "You can click here to download a CSV with hot spots for the currently selected time.",
            href=f"/hot-spots/{month}/{hour}/legs.csv",
            target="__blank",
        )
    ]


@server.route("/hot-spots/<int:month>/<int:hour>/legs.csv")
def hot_spot_csv(month, hour):
    months = queries.months(g.db)
    month = months[month]
    data = queries.legs_for_download(db, month, hour, limit=1000)
    csv = io.StringIO()
    data.to_csv(csv, header=True, index=False)
    return send_file(
        io.BytesIO(csv.getvalue().encode()),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"hot_spots_{month.isoformat()}_{hour}.csv",
    )


@app.callback(
    Output("download-region-csv", "children"),
    Input("month", "value"),
    Input("hour", "value"),
    Input("datasource", "value"),
    Input("line-picker", "value"),
)
def set_download_region_link(month, hour, data_source, line_ref):
    q = f"?line_ref={line_ref}" if line_ref is not None else ""

    return [
        dcc.Link(
            "You can click here to download a CSV with statistics for the currently selected time, data source and line.",
            href=f"/region/{data_source}/{month}/{hour}/legs.csv{q}",
            target="__blank",
        )
    ]


@server.route("/region/<data_source>/<int:month>/<int:hour>/legs.csv")
def region_csv(data_source, month, hour):
    months = queries.months(g.db)
    month = months[month]
    line_ref = request.args.get("line_ref")
    data = queries.legs_for_download(db, month, hour, data_source, line_ref)
    csv = io.StringIO()
    data.to_csv(csv, header=True, index=False)
    return send_file(
        io.BytesIO(csv.getvalue().encode()),
        mimetype="text/csv",
        as_attachment=True,
        download_name=f"{data_source}_{month.isoformat()}_{hour}.csv",
    )


@app.callback(
    Output("rush-intensity", "figure"),
    Input("datasource", "value"),
    Input("month", "value"),
)
def worst_rush_intensity(data_source: str, month: int):
    months = queries.months(g.db)
    month = months[month]
    df = queries.most_rush_intensity(g.db, month, data_source, limit=40)
    return px.bar(
        df.sort_values(by="rush_intensity"),
        x="rush_intensity",
        y="name",
        orientation="h",
        height=800,
        color="hourly_count",
        hover_data=[
            "air_distance_m",
            "hourly_duration",
            "hourly_quartile",
            "monthly_duration",
        ],
    )

@server.route("/ready")
def readycheck():
    return jsonify(dict(
        status="up"
    ))