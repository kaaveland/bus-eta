from datetime import date

from dash import Dash, Output, Input
import plotly.express as px
from flask import g

from . import queries

def add_map_view(app: Dash):
    @app.callback(
        Output("main-map", "figure"),
        Input("datasource", "value"),
        Input("month", "value"),
        Input("hour", "value")
    )
    def render_map(
        data_source: str, month: int, hour: int
    ):
        months = queries.months(g.db)
        month = months[month]
        df = queries.legs(g.db, month, hour, data_source)
        center = dict(
            lat=df.lat.median(), lon=df.lon.median()
        )
        return px.scatter_map(
            df,
            lat='lat',
            lon='lon',
            size='hourly_count',
            hover_name='name',
            hover_data=['monthly_count', 'hourly_duration', 'monthly_duration'],
            center=center,
            zoom=9,
            height=800,
            color='rush_intensity',
            color_continuous_scale="Cividis_r",
            range_color=[1.0, 2.0]
        )