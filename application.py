# pylint: disable=C0103,E0401,C0301
"""
Application code
"""

import copy
import math
import os
import dash
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objs as go
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots
import luts
from gui import layout


# Read data blobs and other items used from env
roses = luts.roses
calms = pd.read_pickle("data/calms.pickle")
exceedance = pd.read_pickle("data/crosswind_exceedance.pickle")
mean_wep = pd.read_pickle("data/mean_wep.pickle")

# separate rose data for different sections
sxs_roses = roses[roses["decade"] != "none"]
roses = roses[roses["decade"] == "none"]

# We set the requests_pathname_prefix to enable
# custom URLs.
# https://community.plot.ly/t/dash-error-loading-layout/8139/6
app = dash.Dash(__name__)

# AWS Elastic Beanstalk looks for application by default,
# if this variable (application) isn't set you will get a WSGI error.
application = app.server
app.index_string = f"""
<!DOCTYPE html>
<html>
    <head>
        <script async defer
            data-website-id="094ecd65-f147-41c8-b740-5f31cdf18755"
            src="https://umami.snap.uaf.edu/script.js"
            data-do-not-track="true"
            data-domains="snap.uaf.edu"
        ></script>
        {{%metas%}}
        <title>{{%title%}}</title>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1">

        <!-- Schema.org markup for Google+ -->
        <meta itemprop="name" content="Historical Winds at Alaska Airports">
        <meta itemprop="description" content="Explore historical wind data for Alaska airports">
        <meta itemprop="image" content="http://snap.uaf.edu/tools/airport-winds/assets/wind-rose.png">

        <!-- Twitter Card data -->
        <meta name="twitter:card" content="summary_large_image">
        <meta name="twitter:site" content="@SNAPandACCAP">
        <meta name="twitter:title" content="Historical Winds at Alaska Airports">
        <meta name="twitter:description" content="Explore historical wind data for Alaska airports">
        <meta name="twitter:creator" content="@SNAPandACCAP">
        <!-- Twitter summary card with large image must be at least 280x150px -->
        <meta name="twitter:image:src" content="http://snap.uaf.edu/tools/airport-winds/assets/wind-rose.png">

        <!-- Open Graph data -->
        <meta property="og:title" content="Historical Winds at Alaska Airports" />
        <meta property="og:type" content="website" />
        <meta property="og:url" content="http://snap.uaf.edu/tools/airport-winds" />
        <meta property="og:image" content="http://snap.uaf.edu/tools/airport-winds/assets/wind-rose.png" />
        <meta property="og:description" content="Explore historical wind data for Alaska airports" />
        <meta property="og:site_name" content="Historical Winds at Alaska Airports" />

        <link rel="alternate" hreflang="en" href="http://snap.uaf.edu/tools/airport-winds" />
        <link rel="canonical" href="http://snap.uaf.edu/tools/airport-winds"/>
        {{%favicon%}}
        {{%css%}}
    </head>
    <body>
        {{%app_entry%}}
        <footer>
            {{%config%}}
            {{%scripts%}}
            {{%renderer%}}
        </footer>
    </body>
</html>
"""
app.title = "WRCC Historical Winds at Alaska Airports"
app.layout = layout


@app.callback(Output("airports-dropdown", "value"), [Input("map", "clickData")])
def update_place_dropdown(selected_on_map):
    """If user clicks on the map, update the drop down."""
    # Look up ID by name -- kind of backwards, but
    # it's because we can't bundle much data into
    # map click handles.
    if selected_on_map is not None:
        c = luts.map_data[
            luts.map_data["real_name"] == selected_on_map["points"][0]["text"]
        ]
        return c.index.tolist()[0]
    # Return a default
    return "PAFA"


@app.callback(
    Output("map", "figure"),
    Input("airports-dropdown", "value"),
)
def update_selected_airport_on_map(sid):
    """Draw a second trace on the map with one community highlighted."""

    return {
        "data": [
            luts.map_airports_trace,
            go.Scattermapbox(
                lat=[luts.map_data.loc[sid]["lat"]],
                lon=[luts.map_data.loc[sid]["lon"]],
                mode="markers",
                marker={"size": 20, "color": "rgb(207, 38, 47)"},
                line={"color": "rgb(0, 0, 0)", "width": 2},
                text=luts.map_data.loc[sid]["real_name"],
                hoverinfo="text",
            ),
        ],
        "layout": luts.map_layout,
    }


def get_rose_calm_sxs_annotations(titles, calm):
    """
    Return a list of correctly-positioned %calm indicators
    for the monthly wind rose charts.
    Take the already-generated list of titles and use
    that pixel geometry to position the %calm info.
    """
    calm_annotations = copy.deepcopy(titles)

    k = 0
    for anno in calm_annotations:
        anno["y"] = anno["y"] - 0.556
        anno["font"] = {"color": "#000", "size": 10}
        calm_text = str(int(round(calm.iloc[k]["percent"] * 100))) + "%"
        if calm.iloc[k]["percent"] > 0.2:
            # If there's enough room, add the "calm" text fragment
            calm_text += " calm"

        anno["text"] = calm_text
        k += 1

    return calm_annotations


def add_runway_traces(sid, fig, height):
    """Add the runway infomation to the crosswinds figure as rectanlges"""

    def add_runway(fig, row):
        name = row.rw_name
        heading = row.rw_heading
        # if heading is > 180, flip
        if heading >= 180:
            heading -= 180

        # use the same trace for runways with same heading, update hoverinfo
        if fig["data"][-2]["meta"] == heading:
            fig["data"][-2]["hovertemplate"] += f"<br>{name}"

            return fig

        xmin, xmax = heading - 3, heading + 3

        lines = go.Scatter(
            {
                "x": [heading, heading],
                "y": [height * 0.01, height - (height * 0.01)],
                "line": {"dash": "dash", "color": "black"},
                "mode": "lines",
                "showlegend": False,
                "hoverinfo": "skip",
            }
        )

        strip = go.Scatter(
            {
                "x": [xmin, xmin, xmax, xmax, xmin],
                "y": [0, height, height, 0, 0],
                "fill": "tozerox",
                "line": {"color": "black", "width": 1},
                "fillcolor": "rgba(211,211,211,0.25)",
                "mode": "lines",
                "showlegend": False,
                "meta": heading,
                "hovertemplate": "Runway heading: %{meta}°<br><br>" + name,
            }
        )

        fig.add_trace(strip)
        fig.add_trace(lines)

        return fig

    airport = luts.airport_meta[luts.airport_meta["sid"] == sid]

    for row in airport.iterrows():
        fig = add_runway(fig, row[1])

    # removes the "trace X" text for runway traces
    for trace in fig["data"][3:]:
        if trace["meta"]:
            trace["hovertemplate"] += "<extra></extra>"

    return fig


@app.callback(
    Output("exceedance_plot", "figure"),
    [Input("airports-dropdown", "value"), Input("units_selector", "value")],
)
def update_exceedance_plot(sid, units):
    """Plot line chart of allowable crosswind threshold exceedance"""
    df = exceedance.loc[exceedance["sid"] == sid]

    station_name = luts.map_data.loc[sid]["real_name"]
    start_year = max(pd.to_datetime(luts.map_data.loc[sid]["begints"]).year, 1980)
    title = f"Runway direction vs. allowable crosswind exceedance, {station_name}, {start_year}-present"

    fig = px.line(
        df,
        x="direction",
        y="exceedance",
        color="threshold",
        custom_data=["rdc_class"],
        color_discrete_sequence=["#DC267F", "#FFB000", "#648FFF"],
        title=title,
        labels={
            "direction": "Runway direction (degrees from true north)",
            "exceedance": "Exceedance frequency (%)",
            "threshold": "Threshold",
        },
    )

    fig.update_layout(
        {
            "plot_bgcolor": "#fff",
            "paper_bgcolor": luts.background_color,
            "yaxis": {
                "gridcolor": "black",
                "showline": True,
                "linecolor": "black",
                "fixedrange": True,
            },
            "xaxis": {
                "showline": True,
                "linecolor": "black",
                "fixedrange": True,
            },
            "font": {"size": 14, "family": "Open Sans"},
            "hovermode": "closest",
            "title_x": 0.5,
            "title_font": {"size": 18},
        }
    )

    fig.update_yaxes(rangemode="tozero")

    fig.update_traces(
        hovertemplate="RDC Class: %{customdata}<br>Runway direction: %{x}°<br>Exceedance frequency: %{y}%"
    )

    fig = add_runway_traces(sid, fig, df["exceedance"].max())

    for i in [0, 1, 2]:
        fig["data"][i]["name"] = luts.exceedance_units[units][fig["data"][i]["name"]]

    return fig


@app.callback(Output("exceedance_plot", "config"), Input("airports-dropdown", "value"))
def update_exceedance_plot_config(sid):
    config = copy.deepcopy(luts.fig_configs)
    config["toImageButtonOptions"]["filename"] = f"{sid}_crosswind_exceedance"

    return config


def get_rose_traces(d, traces, units, showlegend=False, lines=False):
    """
    Get all traces for a wind rose, given the data chunk.
    Month is used to tie the subplot to the formatting
    chunks in the multiple-subplot graph.
    """

    # Directly mutate the `traces` array.
    for sr, sr_info in luts.speed_ranges.items():
        if units in ["kts", "m/s"]:
            name = f"{luts.speed_units[units][sr]} {units}"
        else:
            name = sr + " mph"
        dcr = d.loc[(d["speed_range"] == sr)]
        r_list = dcr["frequency"].tolist()
        theta_list = list(pd.to_numeric(dcr["direction_class"]) * 10)
        props = dict(
            r=r_list,
            theta=theta_list,
            name=name,
            hovertemplate="%{r} %{fullData.name} winds from %{theta}<extra></extra>",
            marker_color=sr_info["color"],
            showlegend=showlegend,
            legendgroup="legend",
        )
        if lines:
            props["mode"] = "lines"
            # append first item of each to close the lines
            props["r"].append(r_list[0])
            props["theta"].append(theta_list[0])
            props["hovertemplate"] = (
                "%{r:.2f}% change in %{fullData.name}<br>winds from %{theta}<extra></extra>"
            )
            traces.append(go.Scatterpolar(props))
        else:
            traces.append(go.Barpolar(props))

    # Compute the maximum extent of any particular
    # petal on the wind rose.
    max_petal = d.groupby(["direction_class"]).sum().max()

    return max_petal


@app.callback(
    Output("rose", "figure"),
    [
        Input("airports-dropdown", "value"),
        Input("units_selector", "value"),
        Input("rose-pcount", "value"),
    ],
)
def update_rose(sid, units, pcount):
    """Generate cumulative wind rose for selected airport"""
    station_name = luts.map_data.loc[sid]["real_name"]
    station_rose = roses.loc[
        (roses["sid"] == sid) & (roses["pcount"] == pcount) & (roses["month"] == 0)
    ]

    traces = []

    get_rose_traces(station_rose, traces, units, True)
    # Compute % calm, use this to modify the hole size
    c = calms.loc[
        (calms["sid"] == sid) & (calms["decade"] == "none") & (calms["month"] == 0)
    ]

    calm = int(round(c["percent"].values[0]))

    start_year = max(pd.to_datetime(luts.map_data.loc[sid]["begints"]).year, 1980)
    rose_layout = {
        "title": dict(
            text=f"Wind Speed/Direction Distribution for {station_name}, {start_year}-present",
            font=dict(size=18),
        ),
        "height": 700,
        "font": dict(family="Open Sans", size=14),
        "margin": {"l": 0, "r": 0, "b": 20, "t": 75},
        "legend": {"orientation": "h", "x": 0, "y": 1},
        "annotations": [
            {
                "x": 0.5,
                "y": 0.5,
                "showarrow": False,
                "text": str(calm) + r"% calm",
                "xref": "paper",
                "yref": "paper",
            }
        ],
        "polar": {
            "legend": {"orientation": "h"},
            "angularaxis": {
                "rotation": 90,
                "direction": "clockwise",
                "tickmode": "array",
                "tickvals": [0, 45, 90, 135, 180, 225, 270, 315],
                "ticks": "",  # hide tick marks
                "ticktext": ["N", "NE", "E", "SE", "S", "SW", "W", "NW"],
                "tickfont": {"color": "#444"},
                "showline": False,  # no boundary circles
                "color": "#888",  # set most colors to #888
                "gridcolor": "#efefef",
            },
            "radialaxis": {
                "color": "#888",
                "gridcolor": "#efefef",
                "ticksuffix": "%",
                "showticksuffix": "last",
                "tickcolor": "rgba(0, 0, 0, 0)",
                "tick0": 0,
                "dtick": {8: 6, 16: 4, 36: 3}[pcount],
                "ticklen": 10,
                "showline": False,  # hide the dark axis line
                "tickfont": {"color": "#444"},
            },
            "hole": calm / 100,
        },
        "paper_bgcolor": luts.background_color,
    }

    return {"layout": rose_layout, "data": traces}


@app.callback(Output("rose", "config"), Input("airports-dropdown", "value"))
def update_rose_config(sid):
    config = copy.deepcopy(luts.fig_configs)
    config["toImageButtonOptions"]["filename"] = f"{sid}_summary_wind_rose"

    return config


def get_rose_calm_month_annotations(titles, calm):
    """
    Return a list of correctly-positioned %calm indicators
    for the monthly wind rose charts.
    Take the already-generated list of titles and use
    that pixel geometry to position the %calm info.
    """
    calm_annotations = copy.deepcopy(titles)

    k = 0
    for anno in calm_annotations:
        anno["y"] = anno["y"] - 0.1225
        anno["font"] = {"color": "#000", "size": 12}
        calm_text = str(int(round(calm.iloc[k]["percent"] * 100))) + "%"
        if calm.iloc[k]["percent"] > 0.2:
            # If there's enough room, add the "calm" text fragment
            calm_text += " calm"

        anno["text"] = calm_text
        k += 1

    return calm_annotations


@app.callback(
    Output("rose_monthly", "figure"),
    [
        Input("airports-dropdown", "value"),
        Input("units_selector", "value"),
        Input("rose-pcount", "value"),
    ],
)
def update_rose_monthly(sid, units, pcount):
    """
    Create a grid of subplots for all monthly wind roses.
    """
    station_name = luts.map_data.loc[sid]["real_name"]
    station_rose = roses.loc[(roses["sid"] == sid) & (roses["pcount"] == pcount)]

    # t = top margin in % of figure.
    subplot_spec = dict(type="polar", t=0.01)
    fig = make_subplots(
        rows=4,
        cols=3,
        horizontal_spacing=0.03,
        vertical_spacing=0.04,
        specs=[
            [subplot_spec, subplot_spec, subplot_spec],
            [subplot_spec, subplot_spec, subplot_spec],
            [subplot_spec, subplot_spec, subplot_spec],
            [subplot_spec, subplot_spec, subplot_spec],
        ],
        subplot_titles=list(luts.months.values()),
    )

    max_axes = pd.DataFrame()
    month = 1
    for i in range(1, 5):
        for j in range(1, 4):
            if_show_legend = month == 1  # only show the first legend
            traces = []
            d = station_rose[station_rose["month"] == month]
            data = get_rose_traces(d, traces, units, if_show_legend)
            if isinstance(data, pd.Series):
                data = data.to_frame().T  # Convert Series to DataFrame

            max_axes = pd.concat([max_axes, data], ignore_index=True)
            for trace in traces:
                fig.add_trace(trace, row=i, col=j)
            month += 1

    # Determine maximum r-axis and r-step.
    # Adding one and using floor(/2.5) was the
    # result of experimenting with values that yielded
    # about 3 steps in most cases, with a little headroom
    # for the r-axis outer ring.
    rmax = max_axes.max()["frequency"] + 1
    rstep = math.floor(rmax / 2.5)

    # Apply formatting to subplot titles,
    # which are actually annotations.
    for i in fig["layout"]["annotations"]:
        i["y"] = i["y"] + 0.01
        i["font"] = dict(size=14, color="#444")
        i["text"] = "<b>" + i["text"] + "</b>"

    # Generate calms.  Subset by community, re-index
    # for easy access, preprocess percent hole size,
    # drop unused columns.
    c = calms.loc[(calms["sid"] == sid) & (calms["month"] != 0)]
    c = c.reset_index()
    c = c.assign(percent=c["percent"] / 100)

    # Get calms as annotations, then merge
    # them into the subgraph title annotations
    fig["layout"]["annotations"] = fig["layout"][
        "annotations"
    ] + get_rose_calm_month_annotations(fig["layout"]["annotations"], c)

    polar_props = dict(
        bgcolor="#fff",
        angularaxis=dict(
            tickmode="array",
            tickvals=[0, 45, 90, 135, 180, 225, 270, 315],
            ticktext=["N", "NE", "E", "SE", "S", "SW", "W", "NW"],
            tickfont=dict(color="#444", size=12),
            showticksuffix="last",
            showline=False,  # no boundary circles
            color="#888",  # set most colors to #888
            gridcolor="#efefef",
            rotation=90,  # align compass to north
            direction="clockwise",  # degrees go clockwise
        ),
        radialaxis=dict(
            color="#888",
            gridcolor="#efefef",
            tickangle=0,
            range=[0, rmax],
            tick0=1,
            dtick=rstep,
            ticksuffix="%",
            showticksuffix="last",
            showline=False,  # hide the dark axis line
            tickfont=dict(color="#444"),
        ),
    )

    start_year = max(pd.to_datetime(luts.map_data.loc[sid]["begints"]).year, 1980)
    fig.update_layout(
        title=dict(
            text=f"Monthly Wind Speed/Direction Distribution for {station_name}, {start_year}-present",
            font=dict(family="Open Sans", size=18),
            x=0.5,
        ),
        margin=dict(l=0, t=100, r=0, b=0),
        font=dict(family="Open Sans", size=12),
        legend=dict(x=0, y=0, orientation="h", font={"size": 14}),
        height=1700,
        # paper_bgcolor="#fff",
        # plot_bgcolor="#fff",
        paper_bgcolor=luts.background_color,
        plot_bgcolor=luts.background_color,
        # We need to explicitly define the rotations
        # we need for each named subplot.
        polar1={**polar_props, **{"hole": c.iloc[0]["percent"]}},
        polar2={**polar_props, **{"hole": c.iloc[1]["percent"]}},
        polar3={**polar_props, **{"hole": c.iloc[2]["percent"]}},
        polar4={**polar_props, **{"hole": c.iloc[3]["percent"]}},
        polar5={**polar_props, **{"hole": c.iloc[4]["percent"]}},
        polar6={**polar_props, **{"hole": c.iloc[5]["percent"]}},
        polar7={**polar_props, **{"hole": c.iloc[6]["percent"]}},
        polar8={**polar_props, **{"hole": c.iloc[7]["percent"]}},
        polar9={**polar_props, **{"hole": c.iloc[8]["percent"]}},
        polar10={**polar_props, **{"hole": c.iloc[9]["percent"]}},
        polar11={**polar_props, **{"hole": c.iloc[10]["percent"]}},
        polar12={**polar_props, **{"hole": c.iloc[11]["percent"]}},
    )

    return fig


@app.callback(Output("rose_monthly", "config"), Input("airports-dropdown", "value"))
def update_monthly_rose_config(sid):
    config = copy.deepcopy(luts.fig_configs)
    config["toImageButtonOptions"]["filename"] = f"{sid}_monthly_wind_rose"

    return config


@app.callback(Output("wep_box", "figure"), [Input("airports-dropdown", "value")])
def update_box_plots(sid):
    """Generate box plot for monthly averages"""

    d = mean_wep.loc[(mean_wep["sid"] == sid)]
    station_name = luts.map_data.loc[sid]["real_name"]
    start_year = max(pd.to_datetime(luts.map_data.loc[sid]["begints"]).year, 1980)

    return go.Figure(
        layout=dict(
            template=luts.plotly_template,
            font=dict(family="Open Sans", size=14),
            title=dict(
                text=f"Average monthly wind energy potential (100m height) for {station_name}, {start_year}-present",
                font=dict(size=18, family="Open Sans"),
                x=0.5,
            ),
            boxmode="group",
            yaxis={
                "title": "Wind energy potential (W/m2)",
                "rangemode": "tozero",
                "fixedrange": True,
            },
            height=550,
            margin={"l": 50, "r": 50, "b": 50, "t": 50, "pad": 4},
            xaxis=dict(
                tickvals=list(luts.months.keys()),
                ticktext=list(luts.months.values()),
                fixedrange=True,
            ),
            plot_bgcolor=luts.background_color,
            paper_bgcolor=luts.background_color,
        ),
        data=[
            go.Box(
                name="",
                fillcolor=luts.speed_ranges["10-14"]["color"],
                x=d.month,
                y=d.wep,
                meta=d.year,
                hovertemplate="%{x} %{meta}: %{y} W/m2",
                marker=dict(color=luts.speed_ranges["22+"]["color"]),
                line=dict(color=luts.speed_ranges["22+"]["color"]),
            )
        ],
    )


@app.callback(Output("wep_box", "config"), Input("airports-dropdown", "value"))
def update_monthly_rose_config(sid):
    config = luts.fig_configs
    config["toImageButtonOptions"]["filename"] = f"{sid}_wind_energy_potential_boxplots"

    return config


# This function should return the filtered data,
# so it can be used by both sxs rose and diff rose
@app.callback(
    Output("comparison-rose-data", "data"),
    [Input("airports-dropdown", "value"), Input("rose-pcount", "value")],
)
def get_comparison_data(sid, pcount):
    """Prep data that will be used in the side-by-side roses and
    the difference polar line chart

    Returns the comparison rose data for the selected station.
    We need the app to hide sxs and diff rose charts if there isn't enough data.
    Use an invisible data container to do that.
    """
    station_name = luts.map_data.loc[sid]["real_name"]
    station_roses = sxs_roses.loc[
        (sxs_roses["sid"] == sid) & (sxs_roses["pcount"] == pcount)
    ]
    # available decades for particular station
    available_decades = station_roses["decade"].unique()
    # if none, return blank template plot
    # with an annotation letting the user know that
    # there's not enough data to display this chart.
    if (len(available_decades) == 0) or ("2010-2019" not in available_decades):
        # empty trace dict to make blank plot
        return {
            "trace_dict": {
                "marker_color": "#fff",
                "marker_line_color": "#fff",
                "r": np.repeat(1, 36),
                "showlegend": False,
                "theta": np.append(np.arange(10, 370, 10), 0),
            },
            "sid": sid,
            "station_name": station_name,
            "anno_dict": {
                "text": f"{station_name} does not have sufficient data for this comparison.",
                "xref": "paper",
                "yref": "paper",
                "x": 0.5,
                "y": 0.5,
                "showarrow": False,
                "bgcolor": "rgba(211,211,211,0.5)",
                "font": {"size": 22},
            },
        }

    # otherwise, return rose and calms data
    # done in for loop to take the earliest decade
    # while preserving other decades in preprocessed data
    for decade in luts.decades.values():
        # select oldest decade and 2010-2019
        if decade in available_decades:
            target_decades = [decade, "2010-2019"]
            data_list = [
                station_roses.loc[station_roses["decade"] == d] for d in target_decades
            ]
            break

    # Generate calms.  Subset by community, re-index
    # for easy access, preprocess percent hole size,
    # drop unused columns.
    station_calms = calms.loc[
        (calms["sid"] == sid) & (calms["decade"] != "none")
    ].reset_index()
    station_calms = station_calms.reset_index()
    station_calms = station_calms.assign(percent=station_calms["percent"] / 100)

    return {
        "data_list": [df.to_dict() for df in data_list],
        "target_decades": target_decades,
        "sid": sid,
        "calms_dict": station_calms.to_dict(),
    }


def make_empty_sxs_rose(empty_trace, subplot_args, rose_layout, rose_dict):
    """Helper function for update_rose_sxs to create empty figure"""
    rose_layout["title"]["text"] = ""

    fig = make_subplots(**subplot_args)
    fig.update_layout(**rose_layout)

    traces = [empty_trace, empty_trace]
    _ = [fig.add_trace(traces[i], row=1, col=(i + 1)) for i in [0, 1]]

    fig.add_annotation(rose_dict["anno_dict"])

    return fig


@app.callback(
    Output("rose_sxs", "figure"),
    [Input("comparison-rose-data", "data"), Input("units_selector", "value")],
)
def update_rose_sxs(rose_dict, units):
    """
    Create side-by-side (sxs) plot of wind roses from different decades
    """
    # initialize figure (display blank fig even if insufficient data)
    # t = top margin in % of figure.
    subplot_spec = dict(type="polar", t=0.02)
    subplot_args = {
        "rows": 1,
        "cols": 2,
        "horizontal_spacing": 0.01,
        "specs": [[subplot_spec, subplot_spec]],
        "subplot_titles": ["", ""],
    }

    # defults for displaying figure regardless of data availability
    polar_props = dict(
        bgcolor="#fff",
        angularaxis=dict(
            tickmode="array",
            tickvals=[0, 45, 90, 135, 180, 225, 270, 315],
            ticktext=["N", "NE", "E", "SE", "S", "SW", "W", "NW"],
            tickfont=dict(color="#444", size=14),
            showticksuffix="last",
            showline=False,  # no boundary circles
            color="#888",  # set most colors to #888
            gridcolor="#efefef",
            rotation=90,  # align compass to north
            direction="clockwise",  # degrees go clockwise
        ),
        radialaxis=dict(
            color="#888",
            gridcolor="#efefef",
            tickangle=0,
            range=[0, 5],
            tick0=1,
            showticklabels=False,
            ticksuffix="%",
            showticksuffix="last",
            showline=False,  # hide the dark axis line
            tickfont=dict(color="#444"),
        ),
    )

    station_name = luts.map_data.loc[rose_dict["sid"]]["real_name"]

    rose_layout = {
        "title": dict(
            text="Historical wind comparison, " + station_name,
            font=dict(family="Open Sans", size=18),
            x=0.5,
        ),
        "margin": dict(l=0, t=100, r=0, b=20),
        "font": dict(family="Open Sans", size=14),
        "legend": {"orientation": "h", "x": -0.05, "y": 1},
        "height": 650,
        "paper_bgcolor": luts.background_color,
        "plot_bgcolor": luts.background_color,
        # We need to explicitly define the rotations
        # we need for each named subplot.
        "polar1": {**polar_props, **{"hole": 0.1}},
        "polar2": {**polar_props, **{"hole": 0.1}},
    }

    if "trace_dict" in rose_dict:
        # this handles case of insufficient data for station
        # trace_dict only present if insufficient data for comparison
        empty_trace = go.Barpolar(rose_dict["trace_dict"])

        return make_empty_sxs_rose(empty_trace, subplot_args, rose_layout, rose_dict)

    subplot_args["subplot_titles"] = rose_dict["target_decades"]
    fig = make_subplots(**subplot_args)

    data_list = [pd.DataFrame(df_dict) for df_dict in rose_dict["data_list"]]
    max_axes = pd.DataFrame()
    for df, show_legend, i in zip(data_list, [True, False], [1, 2]):
        traces = []
        data = get_rose_traces(df, traces, units, show_legend)
        if isinstance(data, pd.Series):
            data = data.to_frame().T  # Convert Series to DataFrame

        max_axes = pd.concat([max_axes, data], ignore_index=True)

        _ = [fig.add_trace(trace, row=1, col=i) for trace in traces]

    # Determine maximum r-axis and r-step.
    # Adding one and using floor(/2.5) was the
    # result of experimenting with values that yielded
    # about 3 steps in most cases, with a little headroom
    # for the r-axis outer ring.
    rmax = max_axes["frequency"].max() + 1
    polar_props["radialaxis"]["range"][1] = rmax
    polar_props["radialaxis"]["dtick"] = math.floor(rmax / 2.5)
    polar_props["radialaxis"]["showticklabels"] = True

    # Apply formatting to subplot titles,
    # which are actually annotations.
    for i in fig["layout"]["annotations"]:
        i["y"] = i["y"] + 0.05
        i["font"] = dict(size=14, color="#444")
        i["text"] = "<b>" + i["text"] + "</b>"

    station_calms = pd.DataFrame(rose_dict["calms_dict"])
    rose_layout["polar1"]["hole"] = station_calms.iloc[0]["percent"]
    rose_layout["polar2"]["hole"] = station_calms.iloc[1]["percent"]

    # Get calms as annotations, then merge
    # them into the subgraph title annotations
    fig["layout"]["annotations"] = fig["layout"][
        "annotations"
    ] + get_rose_calm_sxs_annotations(fig["layout"]["annotations"], station_calms)

    fig.update_layout(**rose_layout)

    return fig


@app.callback(Output("rose_sxs", "config"), Input("comparison-rose-data", "data"))
def update_sxs_rose_config(rose_dict):
    config = copy.deepcopy(luts.fig_configs)
    if "data_list" in rose_dict:
        # if true, then there is sufficient data for comparison roses
        # and can proceed to update filename for download
        sid = rose_dict["sid"]
        config["toImageButtonOptions"]["filename"] = f"{sid}_comparison_wind_rose"
    else:
        # if it's not there, disable download button
        config["modeBarButtonsToRemove"].append("toImage")

    return config


@app.callback(
    Output("rose_diff", "figure"),
    [
        Input("comparison-rose-data", "data"),
        Input("units_selector", "value"),
        Input("rose-pcount", "value"),
    ],
)
def update_diff_rose(rose_dict, units, pcount):
    """Generate difference wind rose by taking difference in
    frequencies of speed/direction bins
    """
    # set up layout info first, used in event that selected station lacks
    # sufficient data for comparison
    station_name = luts.map_data.loc[rose_dict["sid"]]["real_name"]

    rose_layout = {
        "title": dict(
            text="",
            font=dict(size=18),
        ),
        "height": 700,
        "font": dict(family="Open Sans", size=14),
        "margin": {"l": 0, "r": 0, "b": 20, "t": 75},
        "legend": {"orientation": "h", "x": 0, "y": 1},
        "polar": {
            "legend": {"orientation": "h"},
            "angularaxis": {
                "rotation": 90,
                "direction": "clockwise",
                "tickmode": "array",
                "tickvals": [0, 45, 90, 135, 180, 225, 270, 315],
                "ticks": "",  # hide tick marks
                "ticktext": ["N", "NE", "E", "SE", "S", "SW", "W", "NW"],
                "tickfont": {"color": "#444"},
                "showline": False,  # no boundary circles
                "color": "#888",  # set most colors to #888
                "gridcolor": "#efefef",
            },
            "radialaxis": {
                "color": "#888",
                "gridcolor": "#efefef",
                "ticksuffix": "%",
                "showticksuffix": "last",
                "tickcolor": "rgba(0, 0, 0, 0)",
                "tick0": 0,
                "dtick": {8: 2, 16: 2, 36: 1}[pcount],
                "ticklen": 10,
                "showline": False,  # hide the dark axis line
                "tickfont": {"color": "#444"},
            },
            "hole": 0.2,
        },
        "paper_bgcolor": luts.background_color,
    }

    if "trace_dict" in rose_dict:
        # this handles case of insufficient data for station
        # trace_dict only present if insufficient data for comparison
        empty_trace = go.Barpolar(rose_dict["trace_dict"])
        rose_layout["annotations"] = [rose_dict["anno_dict"]]

        return {"layout": rose_layout, "data": [empty_trace]}

    data_list = [pd.DataFrame(df_dict) for df_dict in rose_dict["data_list"]]

    rose_data = data_list[0]
    # compute freuency differences
    rose_data["frequency"] = data_list[1]["frequency"] - rose_data["frequency"]

    traces = []
    get_rose_traces(rose_data, traces, units, True, True)

    station_calms = pd.DataFrame(rose_dict["calms_dict"])
    # compute calm difference
    calm_diff = station_calms.iloc[1]["percent"] - station_calms.iloc[0]["percent"]
    calm_change = luts.calm_diff_lut[calm_diff > 0]
    calm_text = (
        f"calms <b>{calm_change['text']}</b><br>by {abs(round(calm_diff * 100, 1))}%"
    )
    rose_layout["annotations"] = [
        {
            "x": 0.5,
            "y": 0.5,
            "showarrow": False,
            "text": calm_text,
            "xref": "paper",
            "yref": "paper",
        }
    ]
    rose_layout["shapes"] = [
        {
            "type": "circle",
            "x0": 0.455,
            "y0": 0.4,
            "x1": 0.545,
            "y1": 0.6,
            "text": calm_text,
            "xref": "paper",
            "yref": "paper",
            "line": {"color": "#fff"},
            "opacity": calm_diff / 0.2,
            "fillcolor": calm_change["fill"],
        }
    ]

    decade1, decade2 = rose_dict["target_decades"]
    rose_layout["title"][
        "text"
    ] = f"Change in winds from {decade1} to {decade2}, {station_name}"

    return {"layout": rose_layout, "data": traces}


@app.callback(Output("rose_diff", "config"), Input("comparison-rose-data", "data"))
def update_diff_rose_config(rose_dict):
    config = copy.deepcopy(luts.fig_configs)
    if "data_list" in rose_dict:
        # if true, then there is sufficient data for comparison roses
        # and can proceed to update filename for download
        sid = rose_dict["sid"]
        config["toImageButtonOptions"]["filename"] = f"{sid}_change_in_winds"
    else:
        # if it's not there, disable download button
        config["modeBarButtonsToRemove"].append("toImage")

    return config


if __name__ == "__main__":
    application.run(debug=os.getenv("FLASK_DEBUG", default=False), port=8080)
