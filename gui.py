# pylint: disable=C0103,C0301,E0401
"""
GUI code
"""

import os
from datetime import datetime
import plotly.graph_objs as go
from dash import html, dcc
import dash_dangerously_set_inner_html as ddsih
import luts


# For hosting
path_prefix = os.getenv("DASH_REQUESTS_PATHNAME_PREFIX")

map_figure = go.Figure(data=luts.map_airports_trace, layout=luts.map_layout)

# Helper function
def wrap_in_section(content, section_classes="", container_classes="", div_classes=""):
    """
    Helper function to wrap sections.
    Accepts an array of children which will be assigned within
    this structure:
    <section class="section">
        <div class="container">
            <div>[children]...
    """
    return html.Section(
        className="section " + section_classes,
        children=[
            html.Div(
                className="container " + container_classes,
                children=[html.Div(className=div_classes, children=content)],
            )
        ],
    )


def wrap_in_field(label, control, className=""):
    """
    Returns the control wrapped
    in Bulma-friendly markup.
    """
    return html.Div(
        className="field " + className,
        children=[
            html.Label(label, className="label"),
            html.Div(className="control", children=control),
        ],
    )


header = ddsih.DangerouslySetInnerHTML(
    f"""
<header>
<div class="container">
<nav class="navbar" role="navigation" aria-label="main navigation">

  <div class="navbar-brand">
    <a class="navbar-item" href="https://wrcc.dri.edu/">
      <img src="{path_prefix}assets/WRCC.svg">
    </a>

    <a role="button" class="navbar-burger burger" aria-label="menu" aria-expanded="false" data-target="navbarBasicExample">
      <span aria-hidden="true"></span>
      <span aria-hidden="true"></span>
      <span aria-hidden="true"></span>
    </a>
  </div>

  <div class="navbar-menu">

    <div class="navbar-end">
      <div class="navbar-item">
        <div class="buttons">
          <a href="https://uaf-iarc.typeform.com/to/mN7J5cCK#tool=Historical%20Winds%20at%20Alaska%20Airports" class="button is-link" target="_blank">
            Feedback
          </a>
        </div>
      </div>
    </div>
  </div>
</nav>
</div>
</header>
"""
)

about = wrap_in_section(
    [
        ddsih.DangerouslySetInnerHTML(
            """
            <h1 class="title is-3">Historical Winds at Alaska Airports</h1>
            <p class="content is-size-4">Explore visualizations of historical wind data recorded at Alaska airports. To see an airport’s wind data, click a dot on the map or choose from the list. All graphics will update with data from your chosen airport.</p>
            <p class="content is-size-5 camera-icon">Click the <span>
<svg viewBox="0 0 1000 1000" class="icon" height="1em" width="1em"><path d="m500 450c-83 0-150-67-150-150 0-83 67-150 150-150 83 0 150 67 150 150 0 83-67 150-150 150z m400 150h-120c-16 0-34 13-39 29l-31 93c-6 15-23 28-40 28h-340c-16 0-34-13-39-28l-31-94c-6-15-23-28-40-28h-120c-55 0-100-45-100-100v-450c0-55 45-100 100-100h800c55 0 100 45 100 100v450c0 55-45 100-100 100z m-400-550c-138 0-250 112-250 250 0 138 112 250 250 250 138 0 250-112 250-250 0-138-112-250-250-250z m365 380c-19 0-35 16-35 35 0 19 16 35 35 35 19 0 35-16 35-35 0-19-16-35-35-35z" transform="matrix(1 0 0 -1 0 850)"></path></svg>
</span> icon in the upper-right of each chart to download it.</p>
            """
        )
    ],
    section_classes="words-block-grey",
    div_classes="content",
)


def remove_asos_awos(location):
    """remove the ASOS and AWOS strings from station_name field,
    for airports dropdown"""
    location = location.replace("(ASOS)", "")
    location = location.replace("(AWOS)", "")
    return location


def format_location_name(station_name, sid):
    """Uses new location names"""
    if sid in luts.new_location_names:
        return luts.new_location_names[sid]
    else:
        return station_name


airports_dropdown_field = wrap_in_field(
    "Select an airport",
    dcc.Dropdown(
        id="airports-dropdown",
        options=[
            {
                "label": f"{format_location_name(remove_asos_awos(airport.station_name).title(), index)} / {airport.real_name} ({index})",
                "value": index,
            }
            for index, airport in luts.map_data.iterrows()
        ],
        value="PAFA",
    ),
)

map_selector_section = wrap_in_section(
    html.Div(
        children=[
            airports_dropdown_field,
            dcc.Graph(
                id="map",
                figure=map_figure,
                config={"displayModeBar": False, "scrollZoom": False},
            ),
        ],
    ),
    section_classes="roomy",
    container_classes="content",
)

units_radios_field = html.Div(
    className="field radio-selector",
    children=[
        html.Label("Wind speed units", className="label"),
        dcc.RadioItems(
            id="units_selector",
            labelClassName="radio",
            className="control vertical radio",
            options=[
                {"label": "knots", "value": "kts"},
                {"label": "mph", "value": "mph"},
                {"label": "m/s", "value": "m/s"},
            ],
            value="kts",
        ),
    ],
)

rose_res_radios_field = html.Div(
    className="field radio-selector",
    children=[
        html.Label("Wind rose display petals", className="label"),
        dcc.RadioItems(
            id="rose-pcount",
            labelClassName="radio",
            className="control vertical radio",
            options=[
                {"label": "8 (coarse)", "value": 8},
                {"label": "16", "value": 16},
                {"label": "36 (fine)", "value": 36},
            ],
            value=16,
        ),
    ],
)

wind_rose_intro = wrap_in_section(
    ddsih.DangerouslySetInnerHTML(
        """
<h3 class="title is-4">Station Summary</h3>
<p>This wind rose shows prevailing wind direction and speed for all routine hourly data recorded at the selected station.</p>
 <ul>
   <li><strong>Spokes</strong> in the rose point in the compass direction from which the wind was blowing (i.e., a spoke pointing to the right denotes a wind from the east).</li>
   <li><strong>Colors</strong> within each spoke denote wind speed, and segment length denotes occurrence frequency.  Hover cursor over spoke to show the frequencies.</li>
   <li><strong>Size</strong> of the center hole indicates the frequency of calm winds.</li>
 </ul>
"""
    ),
    section_classes="words-block-grey",
    container_classes="content is-size-5",
)

wind_rose_section = wrap_in_section(
    html.Div(
        className="columns",
        children=[
            html.Div(
                className="column is-four-fifths",
                children=[dcc.Graph(id="rose", figure=go.Figure(),),],
            ),
            html.Div(
                className="column is-one-fifth",
                children=[units_radios_field, rose_res_radios_field,],
            ),
        ],
    )
)

monthly_wind_rose_intro = wrap_in_section(
    ddsih.DangerouslySetInnerHTML(
        """
<p>These wind roses are similar to the one shown above, except data are separated by month. Compare the roses to see how wind direction and speed change throughout the year.</p>
"""
    ),
    section_classes="words-block-grey",
    container_classes="content is-size-5",
)

monthly_wind_rose_section = wrap_in_section(
    dcc.Graph(id="rose_monthly", figure=go.Figure(),)
)

crosswind_intro = wrap_in_section(
    ddsih.DangerouslySetInnerHTML(
        """
<h3 class="title is-4">Crosswind Component Calculation</h3>
<p>Use this chart to explore how the allowable crosswind component exceedance changes with runway direction. The exceedance is the frequency with which hourly winds exceeded the allowable crosswind component threshold. Thresholds are derived from the FAA Runway Design Codes (RDC) described in the <a href="https://www.faa.gov/airports/resources/advisory_circulars/index.cfm/go/document.current/documentNumber/150_5300-13">Advisory Circular 150/5300-13A</a> and correspond to different size classes of aircraft. Hover cursor over lines to show aircraft classes. Existing runways are shown, hover cursor over runway corners to see more detail.</p>
  """
    ),
    section_classes="words-block-grey interstitial",
    container_classes="content is-size-5",
)

crosswind_section = wrap_in_section(
    dcc.Graph(id="exceedance_plot", figure=go.Figure(),)
)

wind_energy_intro = wrap_in_section(
    ddsih.DangerouslySetInnerHTML(
        """
<h3 class="title is-4">Wind Energy Potential</h3>
<p>Use this box-plot to explore the seasonal changes in wind energy potential. Each month's average wind energy values are averaged over the period of available data.</p>

 <ul>
     <li>Boxes show the middle 50&percnt; of monthly averages.</li>
     <li>Horizontal lines within boxes show averages based on all hourly reports for a month.</li>
     <li>Whiskers (vertical lines above and below boxes) represent the full ranges of typical variation of monthly averages for the different years, extended to the minimum and maximum points contained within 1.5 of the interquartile range (IQR, which is the height of the box shown).</li>
     <li>Dots indicate outliers, or individual values outside the normal variation (1.5 IQR).</li>
 </ul>

  """
    ),
    section_classes="words-block-grey interstitial",
    container_classes="content is-size-5",
)

wind_energy_section = wrap_in_section(dcc.Graph(id="wep_box", figure=go.Figure(),))

historical_roses_intro = wrap_in_section(
    ddsih.DangerouslySetInnerHTML(
        """
<h3 class="title is-4">Historical Winds Comparison</h3>
<p>Wind roses show prevailing wind direction and speed for two historical decades: &ldquo;recent&rdquo; (2010&ndash;2019) and the oldest decade available.</p>
  """
    ),
    section_classes="words-block-grey interstitial",
    container_classes="content is-size-5",
)

historical_roses_section = wrap_in_section(
    dcc.Graph(id="rose_sxs", figure=go.Figure(),)
)

historical_change_intro = wrap_in_section(
    ddsih.DangerouslySetInnerHTML(
        """
<h3 class="title is-4">Change in Winds</h3>
<p>The chart below displays differences in occurrence frequencies of the various wind classes between the historical decades summarized in the roses above. Use it to further explore the change in winds from then to now.</p>
  """
    ),
    section_classes="words-block-grey interstitial",
    container_classes="content is-size-5",
)

historical_change_section = wrap_in_section(
    dcc.Graph(id="rose_diff", figure=go.Figure(),),
)

help_text = wrap_in_section(
    ddsih.DangerouslySetInnerHTML(
        """
<h3 class="title is-4">About Airport Wind Data</h3>

<p>Wind speed/direction observations source: <a href="https://mesonet.agron.iastate.edu/request/download.phtml?network=AK_ASOS">Iowa Environmental Mesonet</a>, run by Iowa State University. Houses data collected by the <a href="https://www.ncdc.noaa.gov/data-access/land-based-station-data/land-based-datasets/automated-surface-observing-system-asos">Automated Surface Observing System</a> network and the <a href="https://www.ncdc.noaa.gov/data-access/land-based-station-data/land-based-datasets/automated-weather-observing-system-awos">Automated Weather Observing System</a>.</p>
<p>Measurement frequency: Winds were measured hourly in most cases; routine measurements were preferred (nearest to clock hour) in cases where measurements were more frequent.</p>
<p>Observing site criteria: We used data from 166 airport weather stations located across Alaska, selected from a pool of 185 candidate stations in the database. For inclusion here, a station must have a reasonably complete record, and must have begun measurements before June 6, 2010.</p>

<h4 class="title is-4">Data processing and quality control</h4>

<p>Data were adjusted for homogeneity because some instrument heights (now 10 m) and/or precise locations have changed since 1980.</p>
<p>Wind speeds at 47 stations showed a change from one part of the record to the next. Therefore we adjusted the data prior to the change using quantile mapping, a typical method for correcting biased meteorological data.</p>
<p>Five stations displayed two discontinuities. For these, we applied the quantile mapping adjustments to the later period.</p>
<p>We also removed obviously wrong reports (e.g., sustained wind speeds exceeding 110 mph) and short-duration spikes and dips identified using a signal-processing technique for <a href="https://docs.scipy.org/doc/scipy/reference/generated/scipy.signal.find_peaks.html">identifying outliers</a>.</p>

<h4 class="title is-4">Similar tools</h4>

<ul>
    <li>The <a href="http://windtool.accap.uaf.edu/">ACCAP Community Winds tool</a> takes a climatological approach with much of the same data, and includes model-based projections of future winds.</li
</ul>
"""
    ),
    section_classes="words-block-grey",
    container_classes="content is-size-5",
)


# Used in copyright date
current_year = datetime.now().year

footer = html.Footer(
    className="footer",
    children=[
        ddsih.DangerouslySetInnerHTML(
            f"""
<footer class="container">
    <div class="wrapper is-size-6">
        <img src="{path_prefix}assets/UAF.svg"/>
        <div class="wrapped">
            <p>This tool was developed by the <a href="https://uaf-snap.org">Scenarios Network for Alaska & Arctic Planning (SNAP)</a> in collaboration with the <a href="https://wrcc.dri.edu">Western Regional Climate Center</a>. SNAP is a research group at the <a href="https://uaf-iarc.org/">International Arctic Research Center</a> at the <a href="https://uaf.edu/uaf/">University of Alaska Fairbanks</a>.</p>
            <p>Copyright &copy; {current_year} University of Alaska Fairbanks.  All rights reserved.</p>
            <p>UA is an AA/EO employer and educational institution and prohibits illegal discrimination against any individual.  <a href="https://www.alaska.edu/nondiscrimination/">Statement of Nondiscrimination</a> and <a href="https://www.alaska.edu/records/records/compliance/gdpr/ua-privacy-statement/">Privacy Statement</a>.</p>
        </div>
    </div>
</footer>
            """
        ),
    ],
)

layout = html.Div(
    style={"backgroundColor": luts.background_color},
    children=[
        header,
        about,
        map_selector_section,
        wind_rose_intro,
        wind_rose_section,
        monthly_wind_rose_intro,
        monthly_wind_rose_section,
        crosswind_intro,
        crosswind_section,
        wind_energy_intro,
        wind_energy_section,
        historical_roses_intro,
        historical_roses_section,
        historical_change_intro,
        historical_change_section,
        help_text,
        footer,
        dcc.Store(id="comparison-rose-data"),
    ],
)
