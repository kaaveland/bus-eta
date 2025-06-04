import os

import flask
import duckdb
from flask import g, jsonify

from . import api

root = os.environ.get("PARQUET_LOCATION", "data")
db = duckdb.connect(os.path.join(root, "stats.db"), read_only=True)
db.execute("set threads = 2;")
db.execute("set memory_limit = '512MB';")
server = flask.Flask(__name__)
server.register_blueprint(api.app, url_prefix="/api")


@server.before_request
def connect_db():
    g.db = db.cursor()


@server.after_request
def close_db(r):
    g.db.close()
    return r

@server.get("/ready")
def ready():
    return jsonify(dict(state="up"))