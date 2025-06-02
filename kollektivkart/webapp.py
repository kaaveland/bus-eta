import os

import flask
import duckdb
from flask import g

from . import api

root = os.environ.get("PARQUET_LOCATION", "data")
db = duckdb.connect(os.path.join(root, "stats.db"), read_only=True)
server = flask.Flask(__name__)
server.register_blueprint(api.app, url_prefix="/api")


@server.before_request
def connect_db():
    g.db = db.cursor()


@server.after_request
def close_db(r):
    g.db.close()
    return r
