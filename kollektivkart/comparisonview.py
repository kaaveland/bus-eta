from datetime import date

import pandas as pd
from dash import Dash, Output, Input, State, dcc
import plotly.express as px
from flask import g

from . import queries, mapview

_hoverdata_columns = [
    "name",
    "to_stop",
    "net_change_seconds",
    "net_change_proportion",
    "net_change_pct",
    "cur_mean_hourly_duration",
    "prev_mean_hourly_duration",
    "air_distance_meters",
    "cur_hourly_quartile",
    "prev_hourly_quartile",
    "cur_hourly_duration",
    "prev_hourly_duration",
    "cur_hourly_delay",
    "prev_hourly_delay",
    "cur_hourly_deviation",
    "prev_hourly_deviation",
    "cur_hourly_count",
    "prev_hourly_count",
    "data_source",
]


def hovertooltip(hour: int, prev_month: date, cur_month: date) -> str:
    by_ix = {col: i for i, col in enumerate(_hoverdata_columns)}
    now = f"between {hour}:00-{hour + 1}:00"

    def col(c):
        return f"%{{customdata[{by_ix[c]}]}}"

    tooltip = f"""{col("data_source")} - <b>{col("name")}</b> {now}</b><br>
Air distance {col("air_distance_meters")}m<br>
<br>
Changes from {prev_month.strftime("%Y-%m")} to {cur_month.strftime("%Y-%m")}<br>
Typical delay at {col("to_stop")} {col("prev_hourly_delay")}s → {col("cur_hourly_delay")}s<br>
Average travel time {col("prev_mean_hourly_duration")}s → {col("cur_mean_hourly_duration")}s<br>
Changed {col("net_change_seconds")}s ({col("net_change_pct")}% change)<br>
Typical travel time {col("prev_hourly_duration")}s → {col("cur_hourly_duration")}s<br>
75% faster than {col("prev_hourly_quartile")}s → {col("cur_hourly_quartile")}s<br>
Deviation for leg {col("prev_hourly_deviation")}s → {col("cur_hourly_deviation")}s<br>
Counted traffic {col("prev_hourly_count")} → {col("cur_hourly_count")}
"""
    return tooltip


def draw_map(
    df: pd.DataFrame,
    hour: int,
    prev_month: date,
    cur_month: date,
    center: dict[str, float],
    zoom: int,
    title: str,
):
    fig = px.scatter_map(
        df,
        title=title,
        lat="lat",
        lon="lon",
        hover_name="name",
        color="net_change_proportion",
        color_continuous_scale="viridis",
        center=center,
        zoom=zoom,
        height=800,
        hover_data=_hoverdata_columns,
        range_color=[-50, 50]
    )
    fig.update_traces(hovertemplate=hovertooltip(hour, prev_month, cur_month))
    return fig


def comparisonview(app: Dash, state: dcc.Store):
    @app.callback(
        Output("comparison-map", "figure"),
        Output("comparison-state", "data"),
        Input("month", "value"),
        Input("hour", "value"),
        Input("prev-month", "value"),
        Input("comparison-map", "relayoutData"),
        State(state, "data"),
    )
    def render_map(month: int, hour: int, prev_month: int, relayout_data, map_state):
        months = queries.months(g.db)
        cur = months[month]
        prev = months[prev_month]
        if prev > cur:
            prev, cur = cur, prev
        title = f"Travel time comparison between {cur.strftime('%Y-%m')} and {prev.strftime('%Y-%m')} for hour {hour}:00-{hour + 1}:00"
        df = queries.comparisons(g.db, prev, cur, hour, limit=1000)
        map_state = mapview.map_state_from_relayout(map_state, relayout_data)
        return draw_map(
            df,
            hour=hour,
            prev_month=prev,
            cur_month=cur,
            center=map_state["center"],
            zoom=map_state["zoom"],
            title=title,
        ), map_state
