from dash import Dash, Output, Input, State, dcc
import plotly.express as px
from flask import g

from . import queries

def add_map_view(app: Dash, state: dcc.Store):
    @app.callback(
        Output("main-map", "figure"),
        Output("state", "data"),
        Input("datasource", "value"),
        Input("month", "value"),
        Input("hour", "value"),
        Input("main-map", "relayoutData"),
        State(state, "data")
    )
    def render_map(
        data_source: str, month: int, hour: int, relayout_data, map_state
    ):
        months = queries.months(g.db)
        month = months[month]
        df = queries.legs(g.db, month, hour, data_source)
        # This stores the zoom and center of the map on the client so that we can recover it
        # if they change the year-month, stat or hour
        if relayout_data and "map.center" in relayout_data:
            map_state["center"] = relayout_data["map.center"]
        if relayout_data and "map.zoom" in relayout_data:
            map_state["zoom"] = relayout_data["map.zoom"]

        return px.scatter_map(
            df,
            lat='lat',
            lon='lon',
            size='hourly_count',
            hover_name='name',
            hover_data=['monthly_count', 'hourly_duration', 'monthly_duration'],
            center=map_state["center"],
            zoom=map_state["zoom"],
            height=800,
            color='rush_intensity',
            color_continuous_scale="Cividis_r",
            range_color=[1.0, 2.0]
        ), map_state