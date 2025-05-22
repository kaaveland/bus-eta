import pandas as pd
from dash import Dash, Output, Input, State, dcc
import plotly.express as px
from flask import g

from . import queries, mapview


def draw_map(
    df: pd.DataFrame,
    hour: int,
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
        color_continuous_scale="cividis_r",
        center=center,
        zoom=zoom,
        height=800,
        hover_data=[
            "net_change_seconds",
            "net_change_proportion",
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
        ],
    )
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
        title = f"Travel time comparison between {cur.strftime('%Y-%m')} and {prev.strftime('%Y-%m')} for hour {hour}:00-{hour + 1}:00"
        df = queries.comparisons(g.db, prev, cur, hour, limit=1000)
        map_state = mapview.map_state_from_relayout(map_state, relayout_data)
        return draw_map(
            df, hour, map_state["center"], map_state["zoom"], title
        ), map_state
