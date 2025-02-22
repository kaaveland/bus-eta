#!/usr/bin/env python
"""
Start the plotly dash webapp in development mode.

NB! This file isn't pretty.
"""

from dash import dcc, html, Input, Output, Dash, State
from dash.exceptions import PreventUpdate
import plotly.express as px
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import duckdb


def initialize_duckb():
    """Set up an inmemory duckdb and read the parquet files into tables"""
    con = duckdb.connect(database=":memory:")
    con.execute("CREATE TABLE stop_stats AS SELECT * FROM 'stop_stats.parquet';")
    con.execute("CREATE TABLE leg_stats AS SELECT * FROM 'leg_stats.parquet';")
    con.execute("CREATE TABLE stop_line AS SELECT * FROM 'stop_line.parquet';")
    return con


db = initialize_duckb()

# TODO: This is way too pragmatic, verbose, full of global state and not very nice.
# Consider rewriting into fewer queries and wrapping stuff the contents into a dataclass.
# All of this needs to run once only, since data does not change at runtime.
with db.cursor() as cursor:
    df_year_month = cursor.sql("""
    SELECT DISTINCT year, month
    FROM stop_stats
    WHERE (year, month) > (2023, 6)
    ORDER BY year ASC, month ASC
    """).df()

    about = (
        cursor.sql("""
        SELECT 
      SUM(count) :: int as transits_seen
    FROM leg_stats 
    """)
        .df()
        .iloc[0]
        .to_dict()
    )

    memory_usage = cursor.sql("""
        SELECT ROUND(SUM(memory_usage_bytes :: double) / 1e9, 2)
        FROM duckdb_memory()
    """).fetchall()[0][0]

    datapoints_by_year_month = cursor.sql("""
    SELECT 
      SUM(count) :: int as count,
      MAKE_DATE(year, month, 1) as date
    FROM leg_stats
    GROUP BY year, month
    ORDER BY year, month
    """).df()

    datapoints_by_hour = cursor.sql("""
    SELECT 
      SUM(count) :: int as count,
      hour
    FROM leg_stats
    GROUP BY hour
    ORDER BY hour
    """).df()

    unique_stops = (
        cursor.sql("""
    select distinct stop from stop_stats order by stop
    """)
        .df()["stop"]
        .tolist()
    )
    unique_lines = (
        cursor.sql("""
    select distinct lineRef from stop_line order by lineRef
    """)
        .df()["lineRef"]
        .tolist()
    )

    number_of_stop_combinations = cursor.sql(
        "SELECT COUNT(DISTINCT (previous_stop, stop)) FROM leg_stats"
    ).fetchall()[0][0]


app = Dash(
    name="bus-eta",
    title="Public transit study",
)

year_month_values = [
    f"{row.year:04d}-{row.month:02d}" for _, row in df_year_month.iterrows()
]

# Stored on the client so we don't lose track of what the browser is looking at between swapping visualizations
state = dcc.Store(
    id="state",
    data=dict(
        year_month=len(year_month_values) - 1,
        hour=7,
        zoom=10,
        # This centers near Trondheim (median lat and lon from dataset)
        center=dict(lat=63.405012, lon=10.429692),
        stat="deviation",
        chosen_stop=None,
        chosen_leg=None,
    ),
)

# Inputs
date_slider = dcc.Slider(
    id="map-year-month-slider",
    min=0,
    max=len(year_month_values) - 1,
    step=1,
    marks={i: label for i, label in enumerate(year_month_values)},
    value=len(year_month_values) - 1,
)
hour_marks = {i: str(i) for i in range(24)}

hour_slider = dcc.Slider(
    id="hour-slider", min=0, max=23, step=1, marks=hour_marks, value=7
)
stat_labels = {
    "Deviation": "deviation",
    "Delay": "delay",
    "Actual Duration": "actual_duration",
}
stat_picker = dcc.RadioItems(
    id="stat-picker", options=list(stat_labels), value="Deviation", inline=True
)

global_inputs = [
    html.Label("Select year and month", htmlFor=date_slider.id),
    date_slider,
    # Hour slider
    html.Label("Select hour of day", htmlFor=hour_slider.id),
    hour_slider,
    html.Br(),
    html.Label("Select metric", htmlFor=stat_picker.id),
    stat_picker,
    html.Br(),
]

stop_picker = dcc.Dropdown(id="stop-picker", placeholder="Start typing to search")
leg_picker = dcc.Dropdown(id="leg-picker", placeholder="Start typing to search")
line_picker = dcc.Dropdown(id="line-picker", placeholder="Start typing to search")


@app.callback(Output(stop_picker.id, "options"), Input(stop_picker.id, "search_value"))
def complete_stop_options(search_value):
    if not search_value:
        raise PreventUpdate
    return {
        stop: stop
        for stop in unique_stops
        if stop is not None and stop.startswith(search_value)
    }


@app.callback(Output(leg_picker.id, "options"), Input(leg_picker.id, "search_value"))
def complete_leg_options(search_value):
    if not search_value:
        raise PreventUpdate

    pass


@app.callback(Output(line_picker.id, "options"), Input(line_picker.id, "search_value"))
def complete_line_options(search_value):
    if not search_value:
        raise PreventUpdate
    return {
        line: line for line in unique_lines if line is not None and search_value in line
    }


top = [
    html.H1("Public transit study"),
    html.P(
        children=[
            "All data is from ",
            html.A(href="https://data.entur.no", children="Entur"),
            " open data sets. Source code and exploratory analysis available at ",
            html.A(
                href="https://github.com/kaaveland/bus-eta",
                children="github",
            ),
            " under the MIT license.",
            " There's a blog post at the ",
            html.A(href="https://arktekk.no/blog", children="Arktekk blog"),
            " describing the making of this dashboard.",
        ]
    ),
    html.P(children=["Terms used:"]),
    html.Ul(
        children=[
            html.Li(
                "Deviation is number of seconds longer than scheduled it took to go from one stop to the next"
            ),
            html.Li(
                "Delay between two stops is the number of seconds after schedule overall for a transit"
            ),
            html.Li(
                "Actual duration between two stops is the time it takes for a transit to move between them"
            ),
            html.Li(
                "Rush sensitivity is a ratio of actual duration for an hour divided by typical duration, between stops"
            ),
        ]
    ),
    html.P(
        children=[
            "All time units for these are in seconds, and are aggregate numbers for all transits "
            "observed at or between stops during the selected year and month, in the chosen hour of day."
        ]
    ),
]

about = dcc.Tab(
    label="About the data set",
    value="about",
    children=[
        html.H2("Facts about the data set"),
        html.Ul(
            children=[
                html.Li(f"{len(unique_lines)} transit lines seen."),
                html.Li(f"{len(unique_stops)} transit stop places."),
                html.Li(
                    f"{number_of_stop_combinations} stop-to-stop legs visible in map."
                ),
                html.Li(f"{about['transits_seen']} aggregated transit legs used."),
                html.Li(f"{memory_usage}GB data in server memory."),
                html.Li("TODO: Insert download links"),
            ]
        ),
        dcc.Graph(
            id="by-date",
            figure=px.bar(
                datapoints_by_year_month,
                x="date",
                y="count",
                title="Datapoints by month",
            ),
        ),
        dcc.Graph(
            id="by-hour",
            figure=px.bar(
                datapoints_by_hour,
                x="hour",
                y="count",
                title="Datapoints by hour of day",
            ),
        ),
    ],
)

stop_tab = dcc.Tab(
    id="stop-tab",
    label="Stop",
    children=[
        html.H2("Investigate traffic to a stop"),
        html.Label("Stop to investigate", htmlFor=stop_picker.id),
        stop_picker,
        dcc.Graph(id="one-stop-sources"),
        dcc.Graph(id="one-stop-stat"),
    ],
)


@app.callback(
    Output("one-stop-sources", "figure"),
    Input(date_slider.id, "value"),
    Input(stop_picker.id, "value"),
)
def render_traffic_sources(
    year_month_idx,
    chosen_stop,
):
    selected_label = year_month_values[year_month_idx]
    sel_year, sel_month = map(int, selected_label.split("-"))

    with db.cursor() as cursor:
        df = cursor.sql(
            """
        select
           previous_stop,
           hour,
           count
        from leg_stats
        where year = ? and month = ? and stop = ? 
        """,
            params=(sel_year, sel_month, chosen_stop),
        ).df()

    if chosen_stop is None:
        return px.scatter(title="Provide a valid stop")
    else:
        fig = make_subplots(1, 1, y_title="Transits observed", x_title="Hour")
        for previous in df.previous_stop.unique():
            subset = df.loc[df.previous_stop == previous]
            fig.add_trace(
                go.Scatter(
                    x=subset["hour"],
                    y=subset["count"],
                    name=f"{previous}",
                    mode="lines+markers",
                ),
                row=1,
                col=1,
            )
        fig.update_layout(title_text="Transits seen by previous stop")
        return fig


@app.callback(
    Output("one-stop-stat", "figure"),
    Input(date_slider.id, "value"),
    Input(stat_picker.id, "value"),
    Input(stop_picker.id, "value"),
)
def render_traffic_sources(
    year_month_idx,
    stat,
    chosen_stop,
):
    selected_label = year_month_values[year_month_idx]
    sel_year, sel_month = map(int, selected_label.split("-"))

    if stat not in stat_labels:
        raise ValueError(f"{stat} is not a permitted column")
    column = f"{stat_labels[stat]}_stats"

    with db.cursor() as cursor:
        df = (
            cursor.sql(
                f"""
        select
           previous_stop,
           hour,
           {column}['mean'] as mean
        from leg_stats
        where year = ? and month = ? and stop = ? 
        """,
                params=(sel_year, sel_month, chosen_stop),
            )
            .df()
            .melt(id_vars=["previous_stop", "hour"])
            .rename(columns={"value": stat})
        )

    if chosen_stop is None:
        return px.scatter(title="Provide a valid stop")
    else:
        fig = make_subplots(1, 1, y_title="Seconds", x_title="Hour")
        for previous in df.previous_stop.unique():
            subset = df.loc[df.previous_stop == previous]
            fig.add_trace(
                go.Scatter(
                    x=subset["hour"],
                    y=subset[stat],
                    name=f"{previous}",
                    mode="lines+markers",
                ),
                row=1,
                col=1,
            )
        fig.update_layout(title_text=f"{stat}: mean seconds by previous stop")
        return fig


leg_tab = dcc.Tab(
    id="leg-tab",
    label="Leg",
    children=[
        html.H2("Investigate traffic through one leg"),
        html.Label("Leg to investigate"),
        leg_picker,
    ],
)

line_tab = dcc.Tab(
    id="line-tab",
    label="Line",
    children=[
        html.H2("Investigate traffic by one transit line"),
        html.Label("Line to investigate"),
        line_picker,
    ],
)

# Root level / tabs
app.layout = html.Div(
    [
        html.Div(children=top),
        html.Div(children=global_inputs),
        dcc.Tabs(
            id="tabs",
            value="map-tab",
            children=[
                dcc.Tab(
                    label="Map",
                    value="map-tab",
                    children=[
                        dcc.Graph(id="map-figure"),
                    ],
                ),
                stop_tab,
                leg_tab,
                line_tab,
                about,
            ],
        ),
        state,
        html.Footer(children=[html.P("")]),
    ]
)


@app.callback(
    Output("map-figure", "figure"),
    Output(state.id, "data"),
    Input(date_slider.id, "value"),
    Input(hour_slider.id, "value"),
    Input("map-figure", "relayoutData"),
    Input(stat_picker.id, "value"),
    State(state, "data"),
)
def update_map_page(
    year_month_slider_idx,
    hour_value,
    relayout_data,
    stat,
    map_state,
):
    selected_label = year_month_values[year_month_slider_idx]
    sel_year, sel_month = map(int, selected_label.split("-"))

    if stat not in stat_labels:
        raise ValueError(f"{stat} is not a permitted column")

    column = f"{stat_labels[stat]}_stats"

    query = f"""
        SELECT
            previous_stop || ' to ' || stop as leg, 
            map_lat, 
            map_lon, 
            count,
            {column}['mean'] as mean,
            {column}['min'] as min,
            ({column}['percentiles'])[1] as "10% percentile",
            ({column}['percentiles'])[2] as "25% percentile",
            ({column}['percentiles'])[3] as "50% percentile",
            ({column}['percentiles'])[4] as "75% percentile",
            ({column}['percentiles'])[5] as "90% percentile",
            {column}['max'] as max,
            least(3, round(
                (actual_duration_stats['percentiles'])[4] / median(
                   (actual_duration_stats['percentiles'])[2]) over (
                   partition by previous_stop, stop, year, month
            ), 2)) as rush_sensitivity
        FROM leg_stats
        WHERE year = ?
          AND month = ?
          AND hour = ?
    """
    with db.cursor() as curs:
        df = (
            curs.execute(query, (sel_year, sel_month, hour_value))
            .fetchdf()
            .assign(clamp_mean=lambda df: df["mean"].clip(lower=1))
        )

    if relayout_data and "map.center" in relayout_data:
        map_state["center"] = relayout_data["map.center"]
    if relayout_data and "map.zoom" in relayout_data:
        map_state["zoom"] = relayout_data["map.zoom"]

    fig = px.scatter_map(
        df,
        hover_name="leg",
        lat="map_lat",
        lon="map_lon",
        size="clamp_mean",
        hover_data=[
            "mean",
            "count",
            "rush_sensitivity",
            "10% percentile",
            "25% percentile",
            "50% percentile",
            "75% percentile",
            "90% percentile",
        ],
        color="rush_sensitivity",
        color_continuous_scale="blackbody_r",
        zoom=map_state["zoom"],
        center=(map_state["center"]),
        title=f"{stat} in seconds for {sel_year}/{sel_month:02d} between {hour_value}:00-{hour_value}:59",
        height=800,
    )

    return fig, map_state


if __name__ == "__main__":
    app.run_server(debug=True)
