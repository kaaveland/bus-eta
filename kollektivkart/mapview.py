import pandas as pd
from dash import Dash, Output, Input, State, dcc
import plotly.express as px
from flask import g

from . import queries


def hovertooltip(hour: int) -> tuple[list[str], str]:
    hover_data = [
        "name",
        "air_distance_meters",
        "rush_intensity",
        "hourly_quartile",
        "hourly_duration",
        "monthly_duration",
        "hourly_delay",
        "monthly_delay",
        "hourly_deviation",
        "monthly_deviation",
        "hourly_count",
        "monthly_count",
        "mean_hourly_duration",
        "mean_monthly_duration",
    ]
    by_ix = {col: i for i, col in enumerate(hover_data)}
    this_hour = f"between {hour}:00 and {hour + 1 }:00"

    def col(c, fmt=""):
        return f"%{{customdata[{by_ix[c]}]{fmt}}}"

    tooltip = f"""
<b>{col("name")}</b> {this_hour}<br>
Air distance {col("air_distance_meters")}m<br>
Average {col("mean_hourly_duration")}s travel time now<br><br>
Rush intensity {col("rush_intensity", fmt=':.1f')}<br>
25% of transports take longer than {col("hourly_quartile")}s now<br>
{col("monthly_count")} vehicles recorded, and {col("hourly_count")} {this_hour}<br>
Typical travel time {col("monthly_duration")}s , {col("hourly_duration")}s now<br>
Typical delay is {col("monthly_delay")}s, {col("hourly_delay")}s now<br>
Typical deviation is {col("monthly_deviation")}s, {col("hourly_deviation")}s now
"""

    return hover_data, tooltip


def draw_map(
    df: pd.DataFrame,
    hour: int,
    center: dict[str, float],
    zoom: int,
    title: str,
    range_color_scale_stop=5,
):
    hover_data, tooltip = hovertooltip(hour)
    fig = px.scatter_map(
        df,
        title=title,
        lat="lat",
        lon="lon",
        hover_name="name",
        hover_data=hover_data,
        center=center,
        zoom=zoom,
        height=800,
        color="rush_intensity",
        color_continuous_scale="viridis",
        range_color=[1.0, range_color_scale_stop],
    )
    fig.update_traces(hovertemplate=tooltip)
    fig.update_traces(marker=dict(size=10))
    fig.update_layout(coloraxis_showscale=False)
    return fig


def or_empty(dict_or_none: None | dict[str, object]) -> dict[str, object]:
    return {} if dict_or_none is None else dict_or_none


def map_state_from_relayout(map_state, relayout_data):
    return dict(
        center=or_empty(relayout_data).get("map.center", map_state["center"]),
        zoom=or_empty(relayout_data).get("map.zoom", map_state["zoom"]),
    )


def main_map_view(app: Dash, state: dcc.Store):
    @app.callback(
        Output("main-map", "figure"),
        Output("state", "data"),
        Input("datasource", "value"),
        Input("month", "value"),
        Input("hour", "value"),
        Input("line-picker", "value"),
        Input("main-map", "relayoutData"),
        State(state, "data"),
    )
    def render_map(
        data_source: str,
        month: int,
        hour: int,
        line: str | None,
        relayout_data,
        map_state,
    ):
        months = queries.months(g.db)
        month = months[month]
        name = data_source if line is None else line
        title = (
            f"{name} legs for {month.strftime('%Y-%m')} from {hour}:00 to {hour + 1}:00"
        )
        df = queries.legs(g.db, month, hour, data_source, line)
        map_state = map_state_from_relayout(map_state, relayout_data)

        return draw_map(
            df, hour, map_state["center"], map_state["zoom"], title
        ), map_state


def hot_spots(app: Dash, state: dcc.Store):
    @app.callback(
        Output("hotspot-map", "figure"),
        Output("hot-spot-state", "data"),
        Input("month", "value"),
        Input("hour", "value"),
        Input("hotspot-map", "relayoutData"),
        State(state, "data"),
    )
    def render_map(month: int, hour: int, relayout_data, map_state):
        months = queries.months(g.db)
        month = months[month]
        title = f"Legs with highest rush intensity for {month.strftime('%Y-%m')} from {hour}:00 to {hour + 1}:00"
        df = queries.hot_spots(g.db, month, hour)
        map_state = map_state_from_relayout(map_state, relayout_data)
        return draw_map(
            df, hour, map_state["center"], map_state["zoom"], title
        ), map_state
