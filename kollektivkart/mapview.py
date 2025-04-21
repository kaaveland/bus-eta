import pandas as pd
from dash import Dash, Output, Input, State, dcc
import plotly.express as px
from flask import g

from . import queries

def hovertooltip() -> (list[pd.Series], str):
    hover_data = [
        "name",
        "month",
        "hour",
        "air_distance_km",
        "rush_intensity",
        "hourly_duration",
        "monthly_duration",
        "hourly_delay",
        "monthly_delay",
        "hourly_deviation",
        "monthly_deviation",
        "hourly_count",
        "monthly_count"
    ]
    by_ix = {
        col: i for i, col in enumerate(hover_data)
    }
    def col(c): return f"%{{customdata[{by_ix[c]}]}}"

    tooltip = f"""
{col("name")} distance {col("air_distance_km")}km<br><br>
Data from {col("hour")}:00 to {col("hour")}:59 for {col("month")}<br><br>
{col("monthly_count")} vehicles recorded for this month and {col("hourly_count")} in this hour.<br><br>
Typically {col("monthly_duration")}s travel time, {col("hourly_duration")}s in this hour for rush intensity of {col("rush_intensity")}<br><br>
Typical monthly delay is {col("monthly_delay")}s, {col("hourly_delay")}s for this hour<br><br>
Typical monthly deviation is {col("monthly_deviation")}s, {col("hourly_deviation")}s for this hour<br><br>
"""

    return hover_data, tooltip



def draw_map(
    df: pd.DataFrame, center: dict[str, float], zoom: int, range_color_scale_stop=2.5
):
    hover_data, tooltip = hovertooltip()
    fig = px.scatter_map(
        df,
        lat="lat",
        lon="lon",
        size="hourly_count",
        hover_name="name",
        hover_data=hover_data,
        center=center,
        zoom=zoom,
        height=800,
        color="rush_intensity",
        color_continuous_scale="Cividis_r",
        range_color=[1.0, range_color_scale_stop],
    )
    fig.update_traces(hovertemplate=tooltip)
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
        df = queries.legs(g.db, month, hour, data_source, line)
        map_state = map_state_from_relayout(map_state, relayout_data)

        return draw_map(df, map_state["center"], map_state["zoom"]), map_state


def hot_spots(app: Dash, state: dcc.Store):
    @app.callback(
        Output("hotspot-map", "figure"),
        Input("month", "value"),
        Input("hour", "value"),
        Input("hotspot-map", "relayoutData"),
        State(state, "data"),
    )
    def render_map(month: int, hour: int, relayout_data, map_state):
        months = queries.months(g.db)
        month = months[month]
        df = queries.hot_spots(g.db, month, hour)
        map_state = map_state_from_relayout(map_state, relayout_data)
        return draw_map(df, map_state["center"], map_state["zoom"])
