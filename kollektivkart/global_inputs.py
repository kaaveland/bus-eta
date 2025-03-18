from . import queries
from dash import dcc, html
from duckdb.duckdb import DuckDBPyConnection


def render_global_inputs(db: DuckDBPyConnection) -> html.Div:
    data_sources = dcc.Dropdown(
        id="datasource",
        value="RUT",
        options=queries.datasources_by_name(db)
    )
    months = queries.months(db)
    month_slider = dcc.Slider(
        id="month",
        min=0, max=len(months) - 1,
        step=1,
        marks={
            i: month.isoformat() for i, month in enumerate(months)
        },
        value=len(months) - 1,
        included=False
    )
    hour_slider = dcc.Slider(
        id="hour",
        min=0, max=23,
        step=1,
        marks={i: str(i) for i in range(24)},
        value=15
    )

    return html.Div(children=[
        html.Label("Select data source", htmlFor=data_sources.id),
        data_sources,
        html.Label("Select year and month", htmlFor=month_slider.id),
        month_slider,
        html.Label("Select hour of day", htmlFor=hour_slider.id),
        hour_slider
    ])
