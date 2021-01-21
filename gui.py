import os
import luts
import plotly.graph_objs as go
import dash_core_components as dcc
import dash_html_components as html
import dash_dangerously_set_inner_html as ddsih
from datetime import datetime


# For hosting
path_prefix = os.getenv("REQUESTS_PATHNAME_PREFIX") or "/"

# Change the feedback tool name to create a new Feedback URL for this app
# before launching into production.
feedback_toolname = "CHANGE ME"

map_figure = go.Figure(data=luts.map_communities_trace, layout=luts.map_layout)

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


header = html.Div(
    className="header",
    children=[
        html.Div(
            className="container",
            children=[
                html.Div(
                    className="section header--section",
                    children=[
                        html.Div(
                            className="header--titles",
                            children=[
                                html.H1(
                                    "WRCC Alaska Aviation Wind Tool", className="title is-3"
                                ),
                                html.H2(
                                    "Explore aviation-relevant wind data for Alaska airports",
                                    className="subtitle is-5",
                                ),
                            ],
                        ),
                    ],
                )
            ],
        )
    ],
)

intro = wrap_in_section(
    html.Div(
        # className="section",
        children=[
            html.Div(
                className="container",
                children=[
                    html.Div(
                        className="survey-link",
                        children=[
                            html.P(
                                "Intro text.",
                                className="content is-size-4",
                            ),
                            html.A(
                                "Let us know how we can make this tool better",
                                className="button is-link is-medium",
                                rel="external",
                                target="_blank",
                                # href="https://uaf-iarc.typeform.com/to/hOnb5h",
                                href="replace with typeform link"
                            ),
                        ],
                    )
                ],
            )
        ],
    )
)

communities_dropdown_field = html.Div(
    className="field dropdown-selector",
    children=[
        html.Label("Choose a location", className="label"),
        html.Div(
            className="control",
            children=[
                dcc.Dropdown(
                    id="communities-dropdown",
                    options=[
                        {"label": community.place, "value": index}
                        for index, community in luts.communities.iterrows()
                    ],
                    value="PAFA",
                )
            ],
        ),
    ],
)

form_fields = html.Div(
    className="selectors form",
    children=[
        dcc.Markdown(
            """
            Explore past wind data from airports in Alaska. Start by choosing a specific airport.
""",
            className="content is-size-5",
        ),
        ddsih.DangerouslySetInnerHTML(
            """
<p class="content is-size-5 camera-icon">Click the <span>
<svg viewBox="0 0 1000 1000" class="icon" height="1em" width="1em"><path d="m500 450c-83 0-150-67-150-150 0-83 67-150 150-150 83 0 150 67 150 150 0 83-67 150-150 150z m400 150h-120c-16 0-34 13-39 29l-31 93c-6 15-23 28-40 28h-340c-16 0-34-13-39-28l-31-94c-6-15-23-28-40-28h-120c-55 0-100-45-100-100v-450c0-55 45-100 100-100h800c55 0 100 45 100 100v450c0 55-45 100-100 100z m-400-550c-138 0-250 112-250 250 0 138 112 250 250 250 138 0 250-112 250-250 0-138-112-250-250-250z m365 380c-19 0-35 16-35 35 0 19 16 35 35 35 19 0 35-16 35-35 0-19-16-35-35-35z" transform="matrix(1 0 0 -1 0 850)"></path></svg>
</span> icon in the upper&ndash;right of each chart to download it.</p>
            """
        ),
        communities_dropdown_field,
        dcc.Graph(
            id="map",
            figure=map_figure,
            config={"displayModeBar": False, "scrollZoom": False},
        ),
    ],
)

# form_elements_section = wrap_in_section(
#     html.Div(
#         children=[
#             html.H2("Common form elements", className="title is-2"),
#             html.H4(
#                 "These examples can be copy/pasted where appropriate.",
#                 className="subtitle is-4",
#             ),
#             html.Div(
#                 className="columns",
#                 children=[
#                     html.Div(
#                         className="column",
#                         children=[
#                             html.Div(
#                                 children=[
#                                     # html.Div(
#                                     #     className="section",
#                                     #     children=[html.A(id="toc_location"), form_fields],
#                                     # ),
#                                     html.Div(
#                                         className="section",
#                                         children=[
#                                             # html.A(id="toc_g2"),
#                                             html.H3(
#                                                 "Annual wind speed/direction",
#                                                 className="title is-4 title--rose",
#                                             ),
#                                             dcc.Markdown(
#                                                 """
#     This wind rose shows prevailing wind direction and speed for a given location. Data show annual trends averaged over 35 years of observations (1980&ndash;2014).

#      * **Spokes** in the rose point in the compass direction from which the wind was blowing (i.e., a spoke pointing to the right denotes a wind from the east).
#      * **Colors** within each spoke denote frequencies of wind speed occurrence.  Hover cursor over spoke to show the frequencies.
#      * **Size of the center** hole indicates the &percnt; of calm winds.
#          """,
#                                                 className="content is-size-6",
#                                             ),
#                                             dcc.Graph(
#                                                 id="rose",
#                                                 figure=go.Figure(),
#                                                 config=luts.fig_configs,
#                                             ),
#                                         ],
#                                     ),
#                                 ]
#                             )
#                         ]
#                     ),
#                 ],
#             ),
#         ],
#     )
# )

help_text = html.Div(
    className="section",
    children=[
        html.A(id="toc_about"),
        html.Div(
            className="section",
            children=[
                dcc.Markdown(
                    """

### About wind observation data

 * Wind speed observations source: [Iowa Environmental Mesonet](https://mesonet.agron.iastate.edu/request/download.phtml?network=AK_ASOS), run by Iowa State University. Houses data collected by the [Automated Surface Observing System](https://www.ncdc.noaa.gov/data-access/land-based-station-data/land-based-datasets/automated-surface-observing-system-asos) network and the [Automated Weather Observing System](https://www.ncdc.noaa.gov/data-access/land-based-station-data/land-based-datasets/automated-weather-observing-system-awos).
 * Measurement frequency: Varies between locations, from every 5 minutes to every 3 hours. Winds were measured hourly in most cases; speeds were averaged to the nearest hour in cases where measurements were more frequent.
 * Observing site criteria: We use data from 67 observing sites located across Alaska, mostly at airports (see map). For inclusion here, a station must have made 4 or more hourly wind measurements on at least 75&percnt; of the days during the period 1980&ndash;2014.

##### Data processing and quality control

 * Data were adjusted for homogeneity because some instrument heights (now 10 m) and/or precise locations have changed since 1980.
 * Wind speeds at 28 stations showed a statistically significant change from one part of the record to the next. Therefore we adjusted the data prior to the change using quantile mapping, a typical method for correcting biased meteorological data.
 * Four stations displayed two discontinuities. For these, we applied the quantile mapping adjustments to the later period.
 * We also removed obviously wrong reports (e.g., wind speeds exceeding 100 mph) and short-duration (< 6 hour) spikes in which an hourly wind speed was at least 30 mph greater than in the immediately preceding and subsequent hours.

                """,
                    className="is-size-6 content",
                )
            ],
        ),
    ],
)




columns = wrap_in_section(
    html.Div(
        # className="section charts",
        children=[
            html.Div(
                className="columns",
                children=[
                    # html.Div(className="column is-one-fifth", children=[toc]),
                    html.Div(
                        className="column",
                        children=[
                            html.Div(
                                children=[
                                    html.Div(
                                        className="section",
                                        children=[html.A(id="toc_location"), form_fields],
                                    ),
                                    html.Div(
                                        className="section",
                                        children=[
                                            html.A(id="toc_g2"),
                                            # maybe reorganize?
                                            html.H3(
                                                "Crosswind component calculation",
                                                className="title is-4 title--rose",
                                            ),
                                            dcc.Graph(
                                                id="exceedance_plot",
                                                figure=go.Figure(),
                                                config=luts.fig_configs,
                                            ),
                                            html.H3(
                                                "Historical wind speed/direction comparison",
                                                className="title is-4 title--rose",
                                            ),
                                            dcc.Markdown(
                                                """
    These wind roses show prevailing wind direction and speed for two historical decades: "recent" (2010-2019) and older decades if data availability allow.

     * **Spokes** in the rose point in the compass direction from which the wind was blowing (i.e., a spoke pointing to the right denotes a wind from the east).
     * **Colors** within each spoke denote frequencies of wind speed occurrence.  Hover cursor over spoke to show the frequencies.
     * **Size of the center** hole indicates the &percnt; of calm winds.
         """,
                                                className="content is-size-6",
                                            ),
                                            html.Div(
                                                id="rose-sxs-container", children=[
                                                    dcc.Graph(
                                                        id="rose_sxs",
                                                        figure=go.Figure(),
                                                        config=luts.fig_configs,
                                                    ),
                                                ]
                                            ),
                                            html.Div(
                                                id="rose-diff-container", children=[
                                                    dcc.Graph(
                                                        id="rose_diff",
                                                        figure=go.Figure(),
                                                        config=luts.fig_configs,
                                                    ),
                                                ]
                                            ),
        
                                            html.H3(
                                                "Wind energy potential",
                                                className="title is-4 title--rose",
                                            ),
                                            dcc.Graph(
                                                id="wep_box",
                                                figure=go.Figure(),
                                                config=luts.fig_configs,
                                            ),
                                            dcc.Markdown(
                                                """
    These boxplots display the averaged quantiles over span of available data. 

     * **Spokes** in the rose point in the compass direction from which the wind was blowing (i.e., a spoke pointing to the right denotes a wind from the east).
     * **Colors** within each spoke denote frequencies of wind speed occurrence.  Hover cursor over spoke to show the frequencies.
     * **Size of the center** hole indicates the &percnt; of calm winds.
         """,                               )
                                        ],
                                    ),
                                    html.Hr(),
                                ]
                            ),
                            help_text,
                        ],
                    ),
                ],
            )
        ],
    ), 
    "charts"
)


footer = html.Footer(
    className="footer has-text-centered",
    children=[
        html.Div(
            children=[
                html.A(
                    href="https://snap.uaf.edu",
                    target="_blank",
                    className="level-item",
                    children=[html.Img(src=path_prefix + "assets/SNAP_color_all.svg")],
                ),
                html.A(
                    href="https://uaf.edu/uaf/",
                    target="_blank",
                    className="level-item",
                    children=[html.Img(src=path_prefix + "assets/UAF.svg")],
                ),
            ]
        ),
        dcc.Markdown(
            """
UA is an AA/EO employer and educational institution and prohibits illegal discrimination against any individual. [Statement of Nondiscrimination](https://www.alaska.edu/nondiscrimination/)
            """,
            className="content is-size-6",
        ),
    ],
)

layout = html.Div(
    children=[header, intro, html.Hr(), columns, footer],
)
