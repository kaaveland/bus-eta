import os
import flask
import dash
import duckdb
from flask import g
from dash import html, dcc

from . import initdb
from . import queries
from . import global_inputs
from . import mapview

db = duckdb.connect()
initdb.create_tables(
    db, os.environ.get('PARQUET_LOCATION', 'data')
)
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
    name = "kollektivkart",
    title = "Public transit study",
    server=server,
    external_scripts=external_scripts
)

app.layout = html.Div([
    html.H1("Public transit study"),
    global_inputs.render_global_inputs(db),
    html.Div(
        dcc.Graph(id="main-map")
    )
])

mapview.add_map_view(app)


__all__ = [
    initdb,
    queries,
    app,
    server
]

if __name__ == '__main__':
    app.run_server(debug=True)