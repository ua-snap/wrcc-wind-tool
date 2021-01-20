# pylint: disable=C0103,E0401
"""
Template for SNAP Dash apps.
"""

import copy, math, os
import dash
import luts
import numpy as np
import pandas as pd
import plotly.graph_objs as go
import plotly.express as px
from dash.dependencies import Input, Output
from gui import layout, path_prefix
from plotly.subplots import make_subplots


# Read data blobs and other items used from env
roses = pd.read_pickle("data/roses.pickle")
calms = pd.read_pickle("data/calms.pickle")
exceedance = pd.read_pickle("data/crosswind_exceedance.pickle")
wep_quantiles = pd.read_pickle("data/wep_box_data.pickle")
# monthly_means = pd.read_csv("monthly_averages.csv")
# future_rose = pd.read_csv("future_roses.csv")
# percentiles = pd.read_csv("percentiles.csv", index_col=0)

# We set the requests_pathname_prefix to enable
# custom URLs.
# https://community.plot.ly/t/dash-error-loading-layout/8139/6
app = dash.Dash(__name__, requests_pathname_prefix=path_prefix)

# AWS Elastic Beanstalk looks for application by default,
# if this variable (application) isn't set you will get a WSGI error.
application = app.server
gtag_id = os.environ["GTAG_ID"]
app.index_string = f"""
<!DOCTYPE html>
<html>
    <head>
        <!-- Global site tag (gtag.js) - Google Analytics -->
        <script async src="https://www.googletagmanager.com/gtag/js?id=UA-3978613-12"></script>
        <script>
          window.dataLayer = window.dataLayer || [];
          function gtag(){{dataLayer.push(arguments);}}
          gtag('js', new Date());

          gtag('config', '{gtag_id}');
        </script>
        {{%metas%}}
        <title>{{%title%}}</title>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1">

        <!-- Schema.org markup for Google+ -->
        <meta itemprop="name" content="Alaska Community Wind Tool">
        <meta itemprop="description" content="Explore historical wind data for Alaska communities">
        <meta itemprop="image" content="http://windtool.accap.uaf.edu/assets/wind-rose.png">

        <!-- Twitter Card data -->
        <meta name="twitter:card" content="summary_large_image">
        <meta name="twitter:site" content="@SNAPandACCAP">
        <meta name="twitter:title" content="Alaska Community Wind Tool">
        <meta name="twitter:description" content="Explore historical wind data for Alaska communities">
        <meta name="twitter:creator" content="@SNAPandACCAP">
        <!-- Twitter summary card with large image must be at least 280x150px -->
        <meta name="twitter:image:src" content="http://windtool.accap.uaf.edu/assets/wind-rose.png">

        <!-- Open Graph data -->
        <meta property="og:title" content="Alaska Community Wind Tool" />
        <meta property="og:type" content="website" />
        <meta property="og:url" content="http://windtool.accap.uaf.edu" />
        <meta property="og:image" content="http://windtool.accap.uaf.edu/assets/wind-rose.png" />
        <meta property="og:description" content="Explore historical wind data for Alaska communities" />
        <meta property="og:site_name" content="Alaska Community Wind Tool" />

        <link rel="alternate" hreflang="en" href="http://windtool.accap.uaf.edu" />
        <link rel="canonical" href="http://windtool.accap.uaf.edu"/>
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
app.title = "WRCC Alaska Winds"
app.layout = layout


@app.callback(Output("communities-dropdown", "value"), [Input("map", "clickData")])
def update_place_dropdown(selected_on_map):
    """ If user clicks on the map, update the drop down. """

    # Look up ID by name -- kind of backwards, but
    # it's because we can't bundle much data into
    # map click handles.
    # TODO look at customdata property here
    if selected_on_map is not None:
        c = luts.communities[
            luts.communities["place"] == selected_on_map["points"][0]["text"]
        ]
        return c.index.tolist()[0]
    # Return a default
    return "PAFA"


@app.callback(Output("map", "figure"), [Input("communities-dropdown", "value")])
def update_selected_community_on_map(community):
    """ Draw a second trace on the map with one community highlighted. """
    return {
        "data": [
            luts.map_communities_trace,
            go.Scattermapbox(
                lat=[luts.communities.loc[community]["latitude"]],
                lon=[luts.communities.loc[community]["longitude"]],
                mode="markers",
                marker={"size": 20, "color": "rgb(207, 38, 47)"},
                line={"color": "rgb(0, 0, 0)", "width": 2},
                text=luts.communities.loc[community]["place"],
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
        # anno["y"] = anno["y"] - 0.01
        anno["font"] = {"color": "#000", "size": 10}

        # if indexing fails, insufficient data
        try:
            calm_text = str(int(round(calm.iloc[k]["percent"] * 100))) + "%"
            if calm.iloc[k]["percent"] > 0.2:
                # If there's enough room, add the "calm" text fragment
                calm_text += " calm"
        except IndexError:
            calm_text = ""

        anno["text"] = calm_text
        k += 1

    return calm_annotations


def get_rose_traces(d, traces, showlegend=False):
    """
    Get all traces for a wind rose, given the data chunk.
    Month is used to tie the subplot to the formatting
    chunks in the multiple-subplot graph.
    """

    # Directly mutate the `traces` array.
    for sr, sr_info in luts.speed_ranges.items():
        dcr = d.loc[(d["speed_range"] == sr)]
        props = dict(
            r=dcr["frequency"].tolist(),
            theta=pd.to_numeric(dcr["direction_class"]) * 10,
            name=sr + " mph",
            hovertemplate="%{r} %{fullData.name} winds from %{theta}<extra></extra>",
            marker_color=sr_info["color"],
            showlegend=showlegend,
            legendgroup="legend",
        )
        traces.append(go.Barpolar(props))

    # Compute the maximum extent of any particular
    # petal on the wind rose.
    max_petal = d.groupby(["direction_class"]).sum().max()

    return max_petal


@app.callback(
    Output("exceedance_plot", "figure"), [Input("communities-dropdown", "value")]
)
def update_exceedance_plot(community):
    """Plot line chart of allowable crosswind threshold exceedance"""
    df = exceedance.loc[exceedance["sid"] == community]

    title = "Test Allowable crosswind component exceedance"
    fig = px.line(df, x="direction", y="exceedance", color="threshold", title=title)
    fig.update_layout({"plot_bgcolor": "rgba(0, 0, 0, 0)", "yaxis.gridcolor": "black"})

    return fig


@app.callback(Output("rose", "figure"), [Input("communities-dropdown", "value")])
def update_rose(community):
    """ Generate cumulative wind rose for selected community """
    traces = []

    # Subset for community & 0=year
    # d = data.loc[(data["sid"] == community) & (data["month"] == 0)]
    # month not used in these data, for now

    d = data.loc[data["sid"] == community]
    get_rose_traces(d, traces, True)
    # Compute % calm, use this to modify the hole size
    c = calms[calms["sid"] == community]
    # c_mean = c.mean()
    # c_mean = int(round(c_mean["percent"]))

    calm = int(round(c["percent"].values[0]))

    c_name = luts.communities.loc[community]["place"]

    rose_layout = {
        "title": dict(
            text="Annual Wind Speed/Direction Distribution, 1980-2014, " + c_name,
            font=dict(size=18),
        ),
        "height": 700,
        "font": dict(family="Open Sans", size=10),
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
                "dtick": 3,
                "ticklen": 10,
                "showline": False,  # hide the dark axis line
                "tickfont": {"color": "#444"},
            },
            "hole": calm / 100,
        },
    }

    return {"layout": rose_layout, "data": traces}


@app.callback(Output("rose_sxs", "figure"), [Input("communities-dropdown", "value")])
def update_rose_sxs(sid):
    """
    Create side-by-side (sxs) plot of wind roses from different decades
    """
    station_name = luts.communities.loc[sid]["place"]
    # initialize figure (display blank fig even if insufficient data)
    # t = top margin in % of figure.
    subplot_spec = dict(type="polar", t=0.02)
    subplot_args = {
        "rows": 1,
        "cols": 2,
        "horizontal_spacing": 0.03,
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
            tickfont=dict(color="#444", size=10),
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
            dtick=10,
            ticksuffix="%",
            showticksuffix="last",
            showline=False,  # hide the dark axis line
            tickfont=dict(color="#444"),
        ),
    )

    layout = {
        "title": dict(
            text="Historical change in winds, " + station_name,
            font=dict(family="Open Sans", size=18),
            x=0.5,
        ),
        "margin": dict(l=0, t=100, r=0, b=0),
        "font": dict(family="Open Sans", size=10),
        "legend": dict(x=0, y=0, orientation="h"),
        "height": 700,
        "paper_bgcolor": "#fff",
        "plot_bgcolor": "#fff",
        # We need to explicitly define the rotations
        # we need for each named subplot.
        # TODO is there a more elegant way to
        # generate this list of things?
        "polar1": {**polar_props, **{"hole": 0.1}},
        "polar2": {**polar_props, **{"hole": 0.1}},
    }

    # filter to station and
    station_roses = roses.loc[roses["sid"] == sid]
    # available decades for particular station
    available_decades = station_roses["decade"].unique()
    # if none, return blank template plot
    if len(available_decades) == 0:
        # return blank plot if insufficient data
        layout["title"]["text"] = ""
        # layout["polar1"]["radialaxis"]["marker_line_color"] = "#fff"

        fig = make_subplots(**subplot_args)
        fig.update_layout(**layout)

        empty_trace = go.Barpolar(
            {
                "marker_color": "#fff",
                "marker_line_color": "#fff",
                "r": np.repeat(1, 36),
                "showlegend": False,
                "theta": np.append(np.arange(10, 370, 10), 0),
            }
        )
        traces = [empty_trace, empty_trace]
        _ = [fig.add_trace(traces[i], row=1, col=(i + 1)) for i in [0, 1]]

        fig.add_annotation(
            text=f"{station_name} does not have sufficient data for this comparison.",
            xref="paper",
            yref="paper",
            x=0.5,
            y=0.5,
            showarrow=False,
        )

        return fig

    else:
        for decade in luts.decades.values():
            # select oldest decade and most recent decade
            subplot_titles = [decade, "2010-2019"]
            data_list = [
                station_roses.loc[station_roses["decade"] == d] for d in subplot_titles
            ]
            break

    subplot_args["subplot_titles"] = subplot_titles
    fig = make_subplots(**subplot_args)
    # t = top margin in % of figure.
    # subplot_spec = dict(type="polar", t=0.02)
    # fig = make_subplots(
    #     rows=1,
    #     cols=2,
    #     horizontal_spacing=0.03,
    #     #vertical_spacing=0.04,
    #     specs=[[subplot_spec, subplot_spec]],
    #     subplot_titles=subplot_titles,
    # )

    month = 1
    max_axes = pd.DataFrame()
    for df, show_legend, i in zip(data_list, [True, False], [1, 2]):
        traces = []
        max_axes = max_axes.append(
            get_rose_traces(df, traces, show_legend), ignore_index=True
        )
        _ = [fig.add_trace(trace, row=1, col=i) for trace in traces]

    # max_axes = pd.concat([get_rose_traces(df, traces, show_legend) for df, show_legend in zip(data_list, [True, False])])
    # _ = [fig.add_trace(traces[i], row=1, col=i) for i in np.arange(1, 3)]

    # Determine maximum r-axis and r-step.
    # Adding one and using floor(/2.5) was the
    # result of experimenting with values that yielded
    # about 3 steps in most cases, with a little headroom
    # for the r-axis outer ring.
    rmax = max_axes.max() + 1
    polar_props["radialaxis"]["range"][1] = rmax
    polar_props["radialaxis"]["dtick"] = math.floor(rmax / 2.5)

    # Apply formatting to subplot titles,
    # which are actually annotations.
    for i in fig["layout"]["annotations"]:
        i["y"] = i["y"] + 0.05
        i["font"] = dict(size=14, color="#444")
        i["text"] = "<b>" + i["text"] + "</b>"

    # Generate calms.  Subset by community, re-index
    # for easy access, preprocess percent hole size,
    # drop unused columns.
    station_calms = calms[calms["sid"] == sid].reset_index()
    station_calms = station_calms.reset_index()
    station_calms = station_calms.assign(percent=station_calms["percent"] / 100)

    # Get calms as annotations, then merge
    # them into the subgraph title annotations
    fig["layout"]["annotations"] = fig["layout"][
        "annotations"
    ] + get_rose_calm_sxs_annotations(fig["layout"]["annotations"], station_calms)

    fig.update_layout(**layout)

    # fig.update_layout(
    #     title=dict(
    #         text="Historical change in winds, " + station_name,
    #         font=dict(family="Open Sans", size=18),
    #         x=0.5,
    #     ),
    #     margin=dict(l=0, t=100, r=0, b=0),
    #     font=dict(family="Open Sans", size=10),
    #     legend=dict(x=0, y=0, orientation="h"),
    #     height=700,
    #     paper_bgcolor="#fff",
    #     plot_bgcolor="#fff",
    #     # We need to explicitly define the rotations
    #     # we need for each named subplot.
    #     # TODO is there a more elegant way to
    #     # generate this list of things?
    #     polar1={**polar_props, **{"hole": station_calms.iloc[0]["percent"]}},
    #     polar2={**polar_props, **{"hole": station_calms.iloc[1]["percent"]}},
    # )

    return fig


@app.callback(Output("wep_box", "figure"), [Input("communities-dropdown", "value")])
def update_box_plots(community):
    """ Generate box plot for monthly averages """

    d = wep_quantiles.loc[(wep_quantiles["sid"] == community)]
    c_name = luts.communities.loc[community]["place"]

    return go.Figure(
        layout=dict(
            font=dict(family="Open Sans", size=10),
            title=dict(
                text="Wind energy potential (speed cubed, currently) for " + c_name,
                font=dict(size=18, family="Open Sans"),
                x=0.5,
            ),
            boxmode="group",
            yaxis={
                "title": "Cube of wind speed (mph)",
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
        ),
        data=[
            go.Box(
                name="placeholder",
                fillcolor=luts.speed_ranges["10-14"]["color"],
                x=d.month,
                y=d.wep,
                hovertemplate="%{x} %{y} mph",
                marker=dict(color=luts.speed_ranges["22+"]["color"]),
                line=dict(color=luts.speed_ranges["22+"]["color"]),
            )
        ],
    )


if __name__ == "__main__":
    application.run(debug=os.environ["FLASK_DEBUG"], port=8080)
