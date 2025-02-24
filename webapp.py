#!/usr/bin/env python
"""
Start the plotly dash webapp in development mode.
"""

# NB! This file isn't pretty, it's hackathon quality at most. At the very least it should
# be split into one file per tab in the dashboard. I timeboxed this into two very hectic
# weekend days to test as many ideas as possible.

import os

from dash import dcc, html, Input, Output, Dash, State
from dash.exceptions import PreventUpdate
from flask import Flask
import plotly.express as px
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import duckdb
server = Flask(__name__)


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
    unique_legs = (
        cursor.sql(
            "select distinct previous_stop || ' to ' || stop as leg "
            "from leg_stats order by previous_stop, stop"
        )
        .df()["leg"]
        .tolist()
    )

    number_of_stop_combinations = cursor.sql(
        "SELECT COUNT(DISTINCT (previous_stop, stop)) FROM leg_stats"
    ).fetchall()[0][0]

external_scripts = [
    {'src': 'https://scripts.simpleanalyticscdn.com/latest.js', 'async': ''}
] if 'SIMPLE_ANALYTICS' in os.environ else []

app = Dash(name="bus-eta", title="Public transit study", server=server, external_scripts=external_scripts)

year_month_values = [
    f"{row.year:04d}-{row.month:02d}" for _, row in df_year_month.iterrows()
]

# Stored on the client so we don't lose track of what the browser is looking at between swapping visualizations
state = dcc.Store(
    id="state",
    data=dict(
        zoom=10,
        # This centers near Trondheim (median lat and lon from dataset)
        center=dict(lat=63.405012, lon=10.429692),
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
    included=False
)
hour_marks = {i: str(i) for i in range(24)}

hour_slider = dcc.Slider(
    id="hour-slider", min=0, max=23, step=1, marks=hour_marks, value=7, included=False
)
stat_labels = {
    "deviation": "Deviation",
    "delay": "Delay",
    "actual_duration": "Actual duration",
}
stat_picker = dcc.RadioItems(
    id="stat-picker", options=stat_labels, value="deviation", inline=True
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
        if stop is not None and search_value.lower() in stop.lower()
    }


@app.callback(Output(leg_picker.id, "options"), Input(leg_picker.id, "search_value"))
def complete_leg_options(search_value):
    if not search_value:
        raise PreventUpdate

    return {
        leg: leg
        for leg in unique_legs
        if leg is not None and search_value.lower() in leg.lower()
    }


@app.callback(Output(line_picker.id, "options"), Input(line_picker.id, "search_value"))
def complete_line_options(search_value):
    if not search_value:
        raise PreventUpdate
    return {
        line: line
        for line in unique_lines
        if line is not None and search_value.lower() in line.lower()
    }


top = [
    html.H1("Public transit study"),
    html.P(children=["Explanation of terms used on this page:"]),
    html.Ul(
        children=[
            html.Li(
                "A Leg is a travel from one stop to the very next stop. The start of a leg is the arrival time at "
                "the first stop. The end of a leg is the arrival time at the next stop. The markers in the maps "
                "in this dashboard show data about all public transportation that has travelled that leg (all lines). "
                "The markers are placed near the start of the leg."
            ),
            html.Li(
                "Deviation is how much longer it actually took to get from one stop to the next compared to the scheduled time."
            ),
            html.Li(
                "Delay is the total number of seconds by which the vehicle is running late when it arrives at the next stop "
                "(i.e., how far behind schedule it is overall at that point). It is equal to the accumulated deviation up to that stop."
            ),
            html.Li(
                "Actual duration is the real time it takes (in seconds or minutes) for the vehicle to travel from one stop to another."
            ),
            html.Li(
                "Rush sensitivity is a measure that shows how travel time increases during busy hours. It’s calculated by taking the actual "
                "travel time during rush hour and dividing it by the usual (non-rush) travel time. "
                "Intuitively, a rush sensitivity of 2 means that travel between two stops takes twice as much time during rush "
                "traffic."
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

bottom = html.Div(children=[
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
])

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
                html.Li(
                    f"{about['transits_seen']} public transport stop place registrations used."
                ),
                html.Li(f"{memory_usage}GB data in server memory."),
                html.Li("This app uses 3 tables that are available as downloads:"),
                html.Li(
                    children=[
                        html.A(
                            href="https://kaaveland-bus-eta-data.hel1.your-objectstorage.com/leg_stats.parquet",
                            children="leg_stats.parquet",
                        ),
                        " contains aggregations on the from-stop to-stop level.",
                    ]
                ),
                html.Li(
                    children=[
                        html.A(
                            href="https://kaaveland-bus-eta-data.hel1.your-objectstorage.com/stop_stats.parquet",
                            children="stop_stats.parquet",
                        ),
                        " contains aggregations on the stop level.",
                    ]
                ),
                html.Li(
                    children=[
                        html.A(
                            href="https://kaaveland-bus-eta-data.hel1.your-objectstorage.com/stop_line.parquet",
                            children="stop_line.parquet",
                        ),
                        " maps services/lines to which stops they visited in the data set.",
                    ]
                ),
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
        html.Div(id="one-stop-text"),
        dcc.Graph(id="one-stop-sources"),
        dcc.Graph(id="one-stop-stat"),
    ],
)


@app.callback(Output("one-stop-text", "children"), Input(stop_picker.id, "value"))
def show_stop_info(chosen_stop):
    if not chosen_stop or chosen_stop not in unique_stops:
        return html.P("Try searching for a stop. Start typing to clear the selection.")
    with db.cursor() as cursor:
        df = (
            cursor.sql(
                """
        select 
           stop,
           first(map_lat) as latitude,
           first(map_lon) as longitude,
           sum(count) as registrations_total
        from stop_stats
        where stop = ?
        group by stop
        """,
                params=(chosen_stop,),
            )
            .df()
            .T
        )
    return html.Pre(str(df))


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
    year, month = map(int, selected_label.split("-"))

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
            params=(year, month, chosen_stop),
        ).df()

    if chosen_stop is None:
        return px.scatter(title="Provide a valid stop")
    else:
        fig = make_subplots(1, 1, y_title=f"Total in {year}-{month}", x_title="Hour")
        for previous in df.previous_stop.unique():
            subset = df.loc[df.previous_stop == previous]
            fig.add_trace(
                go.Scatter(
                    x=subset["hour"],
                    y=subset["count"],
                    name=f"{previous}",
                    mode="lines+markers",
                    hovertemplate="%{x}:00-%{x}:59 total %{y}",
                ),
                row=1,
                col=1,
            )
        fig.update_layout(
            title_text=f"Transits counted at {chosen_stop} in {year}-{month}"
        )
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
    year, month = map(int, selected_label.split("-"))

    with db.cursor() as cursor:
        df = (
            cursor.sql(
                """
        select
           previous_stop,
           hour,
           (struct_extract(stats, $choice)).mean as mean
        from leg_stats
        where year = $year and month = $month and stop = $stop 
        """,
                params=dict(year=year, month=month, stop=chosen_stop, choice=stat),
            )
            .df()
            .melt(id_vars=["previous_stop", "hour"])
            .rename(columns={"value": stat_labels[stat]})
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
                    y=subset[stat_labels[stat]],
                    name=f"{previous} to {chosen_stop}",
                    mode="lines+markers",
                    hovertemplate="%{x}:00-%{x}:59: %{y} seconds",
                ),
                row=1,
                col=1,
            )
        # My wife forced me to do this, don't blame me
        adjective = "Average accumulated" if stat.lower() == "delay" else "Average"
        fig.update_layout(
            title_text=f"{adjective} {stat.lower()} in seconds in {year}-{month}"
        )
        return fig


leg_tab = dcc.Tab(
    id="leg-tab",
    label="Leg",
    children=[
        html.H2("Investigate traffic through one leg"),
        html.Label("Leg to investigate"),
        leg_picker,
        html.Div(id="one-leg-text"),
        dcc.Graph(id="one-leg-count"),
        dcc.Graph(id="one-leg-stat"),
    ],
)


@app.callback(
    Output("one-leg-text", "children"),
    Input(date_slider.id, "value"),
    Input(leg_picker.id, "value"),
)
def render_leg_text(year_month, chosen_leg):
    if chosen_leg is None:
        return html.P(
            "Try searching for a leg by entering stop names. Start typing to clear the selection."
        )
    selected_label = year_month_values[year_month]
    year, month = map(int, selected_label.split("-"))
    previous, now = chosen_leg.split(" to ")
    with db.cursor() as cursor:
        df = (
            cursor.sql(
                """
        SELECT
             sum(count) as count,
             first(air_distance_km) as air_distance_km,
             map_lat as latidute,
             map_lon as longitude
        FROM leg_stats
        WHERE year = $year
          AND month = $month
          AND previous_stop = $previous AND stop = $stop
        GROUP BY ALL
        LIMIT 1
        """,
                params=dict(year=year, month=month, previous=previous, stop=now),
            )
            .df()
            .T
        )
    return html.Pre(str(df))


@app.callback(
    Output("one-leg-count", "figure"),
    Output("one-leg-stat", "figure"),
    Input(date_slider.id, "value"),
    Input(stat_picker.id, "value"),
    Input(leg_picker.id, "value"),
)
def render_stat_distribution_for_leg(year_month, stat, chosen_leg):
    selected_label = year_month_values[year_month]
    year, month = map(int, selected_label.split("-"))

    if chosen_leg is None:
        return (
            px.scatter(title="Type a valid stop to stop to visualize"),
            px.scatter(title="Type a valid stop to stop to visualize"),
        )

    previous, now = chosen_leg.split(" to ")

    with db.cursor() as cursor:
        df = cursor.sql(
            """
        SELECT
             hour,
             count,
            (stats[$choice].percentiles)[1] as "10% percentile",
            (stats[$choice].percentiles)[2] as "25% percentile",
            (stats[$choice].percentiles)[3] as "50% percentile",
            (stats[$choice].percentiles)[4] as "75% percentile",
            (stats[$choice].percentiles)[5] as "90% percentile",
        FROM leg_stats
        WHERE year = $year
          AND month = $month
          AND previous_stop = $previous AND stop = $stop
        ORDER BY hour
        """,
            params=dict(year=year, month=month, previous=previous, stop=now, choice=stat),
        ).df()

    quantiles = px.line(
        df.drop(columns=["count"]).melt(id_vars="hour", value_name="seconds"),
        x="hour",
        y="seconds",
        color="variable",
        title=f"{stat_labels[stat]} between {previous} and {now} in {year}-{month}",
    )
    counts = px.line(
        df[["hour", "count"]].melt(id_vars="hour", value_name="Count"),
        x="hour",
        y="Count",
        color="variable",
        title=f"Total traffic between {previous} and {now} in {year}-{month} per hour",
    )

    return quantiles, counts


line_tab = dcc.Tab(
    id="line-tab",
    label="Line Map",
    children=[
        html.H2("Investigate traffic by one transit line"),
        html.Label("Line to investigate"),
        line_picker,
        dcc.Graph("line-map"),
    ],
)


@app.callback(
    Output("line-map", "figure"),
    Input(date_slider.id, "value"),
    Input(hour_slider.id, "value"),
    Input(stat_picker.id, "value"),
    Input(line_picker.id, "value"),
)
def update_map_page(
    year_month_slider_idx,
    hour_value,
    stat,
    chosen_line,
):
    selected_label = year_month_values[year_month_slider_idx]
    year, month = map(int, selected_label.split("-"))

    if chosen_line not in unique_lines:
        return px.scatter(title="You must choose a line to see a map here.")

    query = """
        SELECT
            previous_stop || ' to ' || stop as leg, 
            map_lat as latitude, 
            map_lon as longitude, 
            count,
            (struct_extract(stats, $choice))['mean'] as mean,
            (struct_extract(stats, $choice))['min'] as min,
            ((struct_extract(stats, $choice)).percentiles)[1] as "10% percentile",
            ((struct_extract(stats, $choice)).percentiles)[2] as "25% percentile",
            ((struct_extract(stats, $choice)).percentiles)[3] as "50% percentile",
            ((struct_extract(stats, $choice)).percentiles)[4] as "75% percentile",
            ((struct_extract(stats, $choice)).percentiles)[5] as "90% percentile",
            (struct_extract(stats, $choice)).max as max,            
            least(3, round(
                (stats.actual_duration.percentiles)[4] / median(
                   (stats.actual_duration.percentiles)[2]) over (
                   partition by previous_stop, stop, year, month
            ), 2)) as rush_sensitivity
        FROM leg_stats JOIN stop_line using (previous_stop, stop, year, month, hour)
        WHERE year = $year
          AND month = $month
          AND hour = $hour
          AND lineRef = $line
    """
    with db.cursor() as curs:
        df = (
            curs.execute(query,
                         dict(year=year, month=month, hour=hour_value, line=chosen_line, choice=stat))
            .fetchdf()
            .assign(clamp_mean=lambda df: df["mean"].clip(lower=1))
        )

    lat, lon = df.latitude.median(), df.longitude.median()

    fig = px.scatter_map(
        df,
        hover_name="leg",
        lat="latitude",
        lon="longitude",
        size="clamp_mean",
        hover_data={
            "clamp_mean": False,
            "mean": True,
            "count": True,
            "rush_sensitivity": True,
            "10% percentile": True,
            "25% percentile": True,
            "50% percentile": True,
            "75% percentile": True,
            "90% percentile": True,
        },
        color="rush_sensitivity",
        color_continuous_scale="viridis_r",
        zoom=11,
        center=dict(lat=lat, lon=lon),
        title=f"Average {stat_labels[stat].lower()} in seconds for {chosen_line} {year}-{month:02d} between {hour_value}:00-{hour_value}:59",
        height=800,
    )
    fig.update_layout()
    fig.add_annotation(
        text=f"Size represents average {stat_labels[stat].lower()} between {hour_value}:00-{hour_value}:59 for all days in the month",
        xref="paper",
        yref="paper",
        x=0.5,
        y=0,
        font=dict(size=12),
        showarrow=False,
    )

    return fig

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
        bottom
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
    year, month = map(int, selected_label.split("-"))

    query = """
        SELECT
            previous_stop || ' to ' || stop as leg, 
            map_lat as latitude, 
            map_lon as longitude, 
            count,
            (struct_extract(stats, $choice))['mean'] as mean,
            (struct_extract(stats, $choice))['min'] as min,
            ((struct_extract(stats, $choice)).percentiles)[1] as "10% percentile",
            ((struct_extract(stats, $choice)).percentiles)[2] as "25% percentile",
            ((struct_extract(stats, $choice)).percentiles)[3] as "50% percentile",
            ((struct_extract(stats, $choice)).percentiles)[4] as "75% percentile",
            ((struct_extract(stats, $choice)).percentiles)[5] as "90% percentile",
            (struct_extract(stats, $choice)).max as max,
            least(3, round(
                (stats.actual_duration.percentiles)[4] / median(
                   (stats.actual_duration.percentiles)[2]) over (
                   partition by previous_stop, stop, year, month
            ), 2)) as rush_sensitivity
        FROM leg_stats
        WHERE year = $year
          AND month = $month
          AND hour = $hour
    """
    with db.cursor() as curs:
        df = (
            curs.execute(query, dict(year=year, month=month, hour=hour_value, choice=stat))
            .fetchdf()
            .assign(clamp_mean=lambda df: df["mean"].clip(lower=1))
        )

    if relayout_data and "map.center" in relayout_data:
        map_state["center"] = relayout_data["map.center"]
    if relayout_data and "map.zoom" in relayout_data:
        map_state["zoom"] = relayout_data["map.zoom"]
    if relayout_data and "map.tilt" in relayout_data:
        map_state["tilt"] = relayout_data["map.tilt"]

    fig = px.scatter_map(
        df,
        hover_name="leg",
        lat="latitude",
        lon="longitude",
        size="clamp_mean",
        hover_data={
            "clamp_mean": False,
            "mean": True,
            "count": True,
            "rush_sensitivity": True,
            "10% percentile": True,
            "25% percentile": True,
            "50% percentile": True,
            "75% percentile": True,
            "90% percentile": True,
        },
        color="rush_sensitivity",
        color_continuous_scale="viridis_r",
        zoom=map_state["zoom"],
        center=(map_state["center"]),
        title=f"Average {stat_labels[stat].lower()} in seconds for {year}-{month:02d} between {hour_value}:00-{hour_value}:59",
        height=800,
    )
    fig.add_annotation(
        text=f"Size represents average {stat_labels[stat].lower()} between {hour_value}:00-{hour_value}:59 for all days in the month",
        xref="paper",
        yref="paper",
        x=0.5,
        y=0,
        font=dict(size=12),
        showarrow=False,
    )

    return fig, map_state


if __name__ == "__main__":
    app.run_server(debug=True)
