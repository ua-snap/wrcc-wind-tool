# pylint: disable=C0103,C0301,E0401
"""
Contains common lookup tables between GUI/application code
"""

import pandas as pd
import numpy as np
import plotly.graph_objs as go
import plotly.io as pio

# need to get map data ready here first for use in gui
# need to filter to airports meeting minimum data requirements
airport_meta = pd.read_csv("data/airport_meta.csv").set_index("sid")
# load manually-scraped info to update these metadata with airport and runway names
airport_meta = (
    pd.read_csv("data/meta_amend.csv")
    .set_index("sid")
    .combine_first(airport_meta)
    .reset_index()
)
# hardcoded communtiy names to replace values in station_name
new_location_names = {
    "PADK": "Adak",
    "PANC": "Anchorage",
    "PANI": "Aniak",
    "PANT": "Annette",
    "PABR": "Utqiaġvik",
    "PABA": "Kaktovik",
    "PABE": "Bethel",
    "PABT": "Bettles",
    "PALU": "Cape Lisbourne",
    "PAEH": "Cape Newenham",
    "PACZ": "Cape Romanzoff",
    "PALR": "Chandalar",
    "PACV": "Cordova",
    "PADE": "Deering",
    "PAEG": "Eagle",
    "PAEI": "Eielson AFB",
    "PAEL": "Elfin Cove",
    "PAED": "Joint Base Elmendorf-Richardson",
    "PAKF": "False Pass",
    "PABI": "Fort Greely",
    "PAFR": "Joint Base Elmendorf-Richardson",
    "PAGA": "Galena",
    "PAGK": "Gulkana",
    "PAHV": "Healy",
    "PAHO": "Homer",
    "PAOH": "Hoonah",
    "PAHY": "Hydaburg",
    "PAIL": "Iliamna",
    "PAIM": "Utopia Creek",
    "PAJN": "Juneau",
    "PAFE": "Kake",
    "PAKV": "Kaltag",
    "PAEN": "Kenai",
    "PAKT": "Ketchikan",
    "PAVL": "Kivalina",
    "PAOT": "Kotzebue",
    "PAKU": "Kuparuk",
    "PALH": "Anchorage",
    "PAML": "Manley",
    "PAIN": "Denali Park",
    "PAMR": "Anchorage",
    "PAMM": "Metlakatla",
    "PAMD": "Middleton Island",
    "PABN": "Gakona",
    "PPNU": "Nuiqsut",
    "PAAQ": "Palmer",
    "PPIZ": "Point Lay",
    "PAAD": "Deadhorse",
    "PATO": "Girdwood",
    "PAPH": "Port Heiden",
    "PAPT": "Puntilla Lake",
    "PADG": "Red Dog Mine",
    "PASM": "St. Mary's",
    "PASA": "Savoonga",
    "PASY": "Shemya",
    "PASI": "Sitka",
    "PADT": "Slana",
    "PAPB": "St. George Island",
    "PAMK": "St. Michael Island",
    "PATA": "Tanana",
    "PATC": "Shishmaref",
    "PATG": "Togiak",
    "PAFB": "Fort Wainwright",
    "PAWI": "Wainwright",
    "PAWS": "Wasilla",
    "PAUO": "Willow",
    "PAFA": "Fairbanks",
    "PASN": "St. Paul Island",
    "PANN": "Nenana",
}

# remove duplicate rows after discarding runway info to have unique locations
map_data = (
    airport_meta.drop(columns=["rw_name", "rw_heading"])
    .drop_duplicates()
    .set_index("sid")
)
# use only the unique sid values in roses df
roses = pd.read_pickle("data/roses.pickle")
map_data = map_data.loc[map_data.index.isin(roses["sid"].unique())]

# Plotly format template
plotly_template = pio.templates["simple_white"]
axis_configs = {
    "automargin": True,
    "showgrid": False,
    "showline": False,
    "ticks": "",
    "title": {"standoff": 0},
    "zeroline": False,
    "fixedrange": True,
}
xaxis_config = {**axis_configs, **{"tickformat": "%B"}}
plotly_template.layout.xaxis = xaxis_config
plotly_template.layout.yaxis = axis_configs


# This trace is shared so we can highlight specific communities.
map_airports_trace = go.Scattermapbox(
    lat=map_data.loc[:, "lat"],
    lon=map_data.loc[:, "lon"],
    mode="markers",
    marker={"size": 10, "color": "rgb(80,80,80)"},
    line={"color": "rgb(0, 0, 0)", "width": 2},
    text=map_data.real_name,
    hoverinfo="text",
)

map_layout = go.Layout(
    autosize=True,
    hovermode="closest",
    mapbox=dict(style="carto-positron", zoom=3.25, center=dict(lat=63, lon=-158)),
    showlegend=False,
    margin=dict(l=0, r=0, t=0, b=0),
)

# Needs to be a numpy array for ease of building relevant
# strings for some code
# 50, 75, 85, 95, 99
percentiles = np.array(
    [
        "mph (50th %ile) <b>Common<b>",
        "mph (75th %ile)",
        "mph (85th %ile) <b>Occasional</b>",
        "mph (95th %ile)",
        "mph (99th %ile) <b>Rare</b>",
    ]
)

durations = {
    1: "1 continuous hour or more",
    6: "6+ hours",
    12: "12+ hours",
    24: "24+ hours",
    48: "48+ hours",
}

gcms = {"CCSM4": "NCAR-CCSM4", "CM3": "GFDL-CM3"}

months = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}

decades = {
    1980: "1980-1989",
    1990: "1990-1999",
    2000: "2000-2009",
    2010: "2010-2019",
}

# Common configuration for graph figures
fig_download_configs = dict(filename="winds", width="1280", scale=2)
fig_configs = dict(
    displayModeBar=True,
    showSendToCloud=False,
    toImageButtonOptions=fig_download_configs,
    modeBarButtonsToRemove=[
        "zoom2d",
        "pan2d",
        "select2d",
        "lasso2d",
        "zoomIn2d",
        "zoomOut2d",
        "autoScale2d",
        "resetScale2d",
        "hoverClosestCartesian",
        "hoverCompareCartesian",
        "hoverClosestPie",
        "hoverClosest3d",
        "hoverClosestGl2d",
        "hoverClosestGeo",
        "toggleHover",
        "toggleSpikelines",
    ],
    displaylogo=False,
)

# Gradient-colors, from gentlest to darker/more saturated.
# Some charts need to access these directly.
colors = ["#d0d1e6", "#a6bddb", "#74a9cf", "#3690c0", "#0570b0", "#034e7b"]

# The lowest bound excludes actual 0 (calm) readings,
# this is deliberate.
speed_ranges = {
    "0-6": {"range": [0.001, 6], "color": colors[0]},
    "6-10": {"range": [6, 10], "color": colors[1]},
    "10-14": {"range": [10, 14], "color": colors[2]},
    "14-18": {"range": [14, 18], "color": colors[3]},
    "18-22": {"range": [18, 22], "color": colors[4]},
    "22+": {
        "range": [22, 1000],  # let's hope the upper bound is sufficient :-)
        "color": colors[5],
    },
}

# for displaying different units
speed_units = {
    "kts": {
        "0-6": "0-5.2",
        "6-10": "5.2-8.7",
        "10-14": "8.7-12.2",
        "14-18": "12.2-15.7",
        "18-22": "15.7-19.1",
        "22+": "19.1+",
    },
    "m/s": {
        "0-6": "0-2.7",
        "6-10": "2.7-4.5",
        "10-14": "4.5-6.3",
        "14-18": "6.3-8",
        "18-22": "8-9.8",
        "22+": "9.8+",
    },
}

exceedance_units = {
    "mph": {"12.08": "12.1 mph", "14.96": "15 mph", "18.41": "18.4 mph"},
    "kts": {"12.08": "10.5 kts", "14.96": "13 kts", "18.41": "16 kts"},
    "m/s": {"12.08": "5.4 m/s", "14.96": "6.7 m/s", "18.41": "8.2 m/s"},
}

exceedance_classes = [
    "A-I and B-I",
    "A-II and B-II",
    "A-III, B-III, C-I through D-III, D-I through D-III",
]

background_color = "#EFEFEF"

calm_diff_lut = {
    True: {"text": "increased", "fill": "#bbb"},
    False: {"text": "decreased", "fill": speed_ranges["10-14"]["color"]},
}
