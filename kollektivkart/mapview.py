import pandas as pd
from dash import Dash, Output, Input, State, dcc
import plotly.express as px
from flask import g

from . import queries


def draw_map(
    df: pd.DataFrame, center: dict[str, float], zoom: int, range_color_scale_stop=2.5
):
    return px.scatter_map(
        df,
        lat="lat",
        lon="lon",
        size="hourly_count",
        hover_name="name",
        hover_data=["monthly_count", "hourly_duration", "monthly_duration"],
        center=center,
        zoom=zoom,
        height=800,
        color="rush_intensity",
        color_continuous_scale="Cividis_r",
        range_color=[1.0, range_color_scale_stop],
    )


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
