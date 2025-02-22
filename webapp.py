#!/usr/bin/env python

from dash import dcc, html, Input, Output, Dash, State
import plotly.express as px
import duckdb

# Initialization: This isn't threadsafe, it's writing to memory.
# NB! Subsequent queries that may come from threads must use `con.cursor()` for threadsafety
con = duckdb.connect(database=':memory:')
con.execute("CREATE TABLE stop_stats AS SELECT * FROM 'stop_stats.parquet';")
con.execute("CREATE TABLE leg_stats AS SELECT * FROM 'leg_stats.parquet';")
df_year_month = con.sql("""
    SELECT DISTINCT year, month
    FROM stop_stats
    ORDER BY year ASC, month ASC
""").df()


year_month_values = [
    f"{row.year:04d}-{row.month:02d}"
    for _, row in df_year_month.iterrows()
]
slider_marks = {i: label for i, label in enumerate(year_month_values)}
hour_marks = {i: str(i) for i in range(24)}

app = Dash(
    __name__,
    # need this since we have 2 views apparently, otherwise some ids that dash assumes, don't exist
    suppress_callback_exceptions=True
)

map_state = dcc.Store(id='stored-map-state', data=dict(
        zoom=10,
        # Somewhere in Trondheim
        center=dict(lat=63.405012, lon=10.429692),
        year_month=0,
        hour=7
    ))

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    map_state,

    html.Div([
        dcc.Link('Go to Scatter Map View', href='/map', style={'marginRight': '20px'}),
        dcc.Link('Go to Graph View', href='/graph'),
    ], style={'marginBottom': '20px'}),


    html.Div(id='page-content')
])

# ------------------------------------------------------------------------------
# Layout for Graph View
graph_layout = html.Div([
    html.H2('Graph View'),
    dcc.Slider(
        id='graph-year-month-slider',
        min=0,
        max=len(year_month_values) - 1,
        step=1,
        marks=slider_marks,
        value=len(year_month_values) - 1,
    ),
    html.Br(),
    dcc.Graph(id='graph-figure')
])

# Layout for Scatter Map View
map_layout = html.Div([
    html.H2('Scatter Map View'),
    # Year-month slider
    dcc.Slider(
        id='map-year-month-slider',
        min=0,
        max=len(year_month_values) - 1,
        step=1,
        marks=slider_marks,
        value=len(year_month_values) - 1,
    ),
    html.Br(),
    # Hour slider
    html.Label('Hour:'),
    dcc.Slider(
        id='map-hour-slider',
        min=0,
        max=23,
        step=1,
        marks=hour_marks,
        value=7
    ),
    html.Br(),
    dcc.Graph(id='map-figure')
])

@app.callback(Output('page-content', 'children'),
              Input('url', 'pathname'))
def display_page(pathname):
    if pathname == '/map':
        return map_layout
    else:
        # Default or “/graph”
        return graph_layout


@app.callback(
    Output('graph-figure', 'figure'),
    Input('graph-year-month-slider', 'value')
)
def update_graph_page(year_month_slider_idx):
    selected_label = year_month_values[year_month_slider_idx]
    sel_year, sel_month = map(int, selected_label.split('-'))

    # Example: fetch data for that year-month from DuckDB
    query = f"""
        SELECT year, month, COUNT(*) AS count_stops
        FROM stop_stats
        WHERE year = ? AND month = ?
        GROUP BY year, month
    """
    df_query = con.execute(query, (sel_year, sel_month)).fetchdf()

    # Example plot: simple bar showing count_stops
    fig = px.bar(
        df_query,
        x='month',  # or just display a single bar
        y='count_stops',
        title=f"Number of Stops in {sel_year}-{sel_month:02d}"
    )
    return fig

@app.callback(
    Output('map-figure', 'figure'),
    Output('stored-map-state', 'data'),
    Input('map-year-month-slider', 'value'),
    Input('map-hour-slider', 'value'),
    Input('map-figure', 'relayoutData'),
    State('stored-map-state', 'data'),
)
def update_map_page(
        year_month_slider_idx,
        hour_value,
        relayout_data,
        map_state,
        ):

    selected_label = year_month_values[year_month_slider_idx]
    sel_year, sel_month = map(int, selected_label.split('-'))

    query = f"""
        SELECT
            previous_stop || ' -> ' || stop as leg, 
            map_lat, 
            map_lon, 
            count,
            deviation_stats['mean'] as mean,
            (deviation_stats['percentiles'])[1] as "10% percentile",
            (deviation_stats['percentiles'])[2] as "25% percentile",
            (deviation_stats['percentiles'])[3] as "50% percentile",
            (deviation_stats['percentiles'])[4] as "75% percentile",
            (deviation_stats['percentiles'])[5] as "90% percentile",
        FROM leg_stats
        WHERE year = ?
          AND month = ?
          AND hour = ?
    """
    with con.cursor() as curs:
        df = curs.execute(query, (sel_year, sel_month, hour_value)).fetchdf().assign(
            clamp_mean=lambda df: df['mean'].clip(lower=1)
        )
        df.info()

    if relayout_data and 'map.center' in relayout_data:
        map_state['center'] = relayout_data['map.center']
    if relayout_data and 'map.zoom' in relayout_data:
        map_state['zoom'] = relayout_data['map.zoom']

    fig = px.scatter_map(
        df,
        hover_name='leg',
        lat='map_lat',
        lon='map_lon',
        size='clamp_mean',
        hover_data=[
            'mean',
            '10% percentile',
            '25% percentile',
            '50% percentile',
            '75% percentile',
            '90% percentile',
        ],
        color='mean',
        color_continuous_scale='blackbody_r',
        zoom=map_state['zoom'],
        center=(map_state['center']),
        title=f"{sel_year}/{sel_month:02d}: {hour_value}:00-{hour_value}:59",
        height=800
    )

    return fig, map_state

if __name__ == '__main__':
    app.run_server(debug=True)
