from . import queries
from dash import dcc, html
from duckdb.duckdb import DuckDBPyConnection


def render_month_slider(
    db: DuckDBPyConnection, id_: str, default_from_end=1
) -> dcc.Slider:
    months = queries.months(db)
    return dcc.Slider(
        id=id_,
        min=0,
        max=len(months) - 1,
        step=1,
        marks={i: month.isoformat() for i, month in enumerate(months) if i % 2 == 0},
        value=len(months) - default_from_end,
        included=False,
    )


def render_global_inputs(db: DuckDBPyConnection) -> html.Div:
    month_slider = render_month_slider(db, "month")
    hour_slider = dcc.Slider(
        id="hour", min=0, max=23, step=1, marks={i: str(i) for i in range(24)}, value=15
    )

    return html.Div(
        children=[
            html.Label("Select year and month", htmlFor=month_slider.id),
            month_slider,
            html.Label("Select hour of day", htmlFor=hour_slider.id),
            hour_slider,
        ]
    )
