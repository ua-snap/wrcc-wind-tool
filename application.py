# pylint: disable=C0103,E0401
"""
Template for SNAP Dash apps.
"""

import dash
import dash_core_components as dcc
import dash_html_components as html

app = dash.Dash(__name__)
# AWS Elastic Beanstalk looks for application by default,
# if this variable (application) isn't set you will get a WSGI error.
application = app.server

app.title = "SNAP Dash Template"

button_primary = html.Button("Primary", className="button")
button_warning = html.Button("Primary", className="button is-warning")
button_loading = html.Button("Primary", className="button is-info is-loading")

buttons_field = html.Div(
    className="field",
    children=[
        html.Label("Buttons", className="label"),
        html.Div(
            className="buttons control",
            children=[button_primary, button_loading, button_warning],
        ),
        dcc.Markdown(
            "You can see more info on different button styles and classes [here](http://www.somewhere.com)",
            className="help",
        ),
    ],
)

dropdown = dcc.Dropdown(
    options=[
        {"label": "New York City", "value": "NYC"},
        {"label": "Montréal", "value": "MTL"},
        {"label": "San Francisco", "value": "SF"},
    ],
    value="MTL",
)
dropdown_field = html.Div(
    className="field",
    children=[
        html.Label("Dropdown", className="label"),
        html.Div(className="control", children=[dropdown]),
    ],
)

multi_dropdown = dcc.Dropdown(
    options=[
        {"label": "New York City", "value": "NYC"},
        {"label": "Montréal", "value": "MTL"},
        {"label": "San Francisco", "value": "SF"},
    ],
    multi=True,
    value="MTL",
)

multi_dropdown_field = html.Div(
    className="field",
    children=[
        html.Label("Multi-dropdown", className="label"),
        html.Div(className="control", children=[multi_dropdown]),
    ],
)

slider = dcc.Slider(min=-5, max=10, step=0.5, value=-3)
slider_field = html.Div(
    className="field",
    children=[
        html.Label("Slider", className="label"),
        html.Div(className="control", children=[slider]),
    ],
)

range_slider = dcc.RangeSlider(count=1, min=-5, max=10, step=0.5, value=[-3, 7])
range_slider_field = html.Div(
    className="field",
    children=[
        html.Label("Range Slider", className="label"),
        html.Div(className="control", children=[range_slider]),
    ],
)

marked_slider = dcc.Slider(
    min=2000,
    max=2090,
    step=10,
    marks={2000: "2000\u2019s", 2050: "2050\u2019s", 2090: "2090\u2019s"},
    value=2030,
)
marked_slider_field = html.Div(
    className="field",
    children=[
        html.Label("Marked Slider", className="label"),
        html.Div(className="control", children=[marked_slider]),
        html.Br(),
        html.P(
            """
            It's possible that the range marker labels will leak outside of the margins of the column, but they're still centered nicely.  Reduce the width of the slider if needed to keep spacing consistent.
            """,
            className="help",
        ),
    ],
)

checkboxes = dcc.Checklist(
    labelClassName="checkbox",
    className="control",
    options=[
        {"label": " New York City ", "value": "NYC"},
        {"label": " Montréal ", "value": "MTL"},
        {"label": " San Francisco ", "value": "SF"},
    ],
    value=["MTL", "SF"],
)
checkboxes_field = html.Div(
    className="field", children=[html.Label("Checklist", className="label"), checkboxes]
)

radios = dcc.RadioItems(
    labelClassName="radio",
    className="control",
    options=[
        {"label": " New York City ", "value": "NYC"},
        {"label": " Montréal ", "value": "MTL"},
        {"label": " San Francisco ", "value": "SF"},
    ],
    value="MTL",
)
radios_field = html.Div(
    className="field", children=[html.Label("RadioItems", className="label"), radios]
)

form_elements_section = html.Div(
    className="section",
    children=[
        html.H2("Common form elements", className="title is-2"),
        html.H4(
            "These examples can be copy/pasted where appropriate.",
            className="subtitle is-4",
        ),
        html.Div(
            className="columns",
            children=[
                html.Div(
                    className="column",
                    children=[
                        buttons_field,
                        html.Br(),
                        dropdown_field,
                        html.Br(),
                        multi_dropdown_field,
                    ],
                ),
                html.Div(
                    className="column",
                    children=[
                        slider_field,
                        html.Br(),
                        range_slider_field,
                        html.Br(),
                        marked_slider_field,
                    ],
                ),
                html.Div(
                    className="column",
                    children=[checkboxes_field, html.Br(), radios_field],
                ),
            ],
        ),
    ],
)

navbar = html.Div(
    className="navbar",
    role="navigation",
    children=[
        html.Div(
            className="navbar-brand",
            children=[
                html.A(
                    className="navbar-item",
                    href="#",
                    children=[html.Img(src="assets/SNAP_acronym_color.svg")],
                )
            ],
        ),
        html.Div(
            className="navbar-menu",
            children=[
                html.Div(
                    className="navbar-start",
                    children=[
                        html.A("Home", className="navbar-item"),
                        html.A("About", className="navbar-item"),
                        html.P("Navbar text", className="navbar-item"),
                    ],
                ),
                html.Div(
                    className="navbar-end",
                    children=[
                        html.Div(
                            className="navbar-item",
                            children=[
                                html.Div(
                                    className="buttons",
                                    children=[
                                        html.A(
                                            "Sign up", className="button is-primary"
                                        ),
                                        html.A("Log in", className="button is-light"),
                                    ],
                                )
                            ],
                        )
                    ],
                ),
            ],
        ),
    ],
)

typography_section = html.Div(
    className="section",
    children=[
        html.H1("Typography", className="title is-1"),
        html.H3("Titles, text, and basic typography.", className="subtitle is-3"),
        html.Hr(),
        html.Div(
            className="columns",
            children=[
                html.Div(
                    className="column",
                    children=[
                        html.H2("Header (is-2)", className="title is-2"),
                        html.H3(
                            "Subtitle (is-4) is spaced more closely to title. 👍",
                            className="subtitle is-4",
                        ),
                        html.Br(),
                        html.H3("Header (is-3)", className="title is-3"),
                        html.H4(
                            "Subtitle (is-5).  Better spacing!",
                            className="subtitle is-5",
                        ),
                        html.H3("General Subheader (h3)", className="subtitle is-3"),
                        html.H4("General Subheader (h4)", className="subtitle is-4"),
                        html.H5("General Subheader (h5)", className="subtitle is-5"),
                    ],
                ),
                html.Div(
                    className="column",
                    children=[
                        dcc.Markdown(
                            """
*Paragraph text*, `is-size-4`.  **Boutique eclectic** Asia-Pacific efficient charming airport liveable the highest quality Ginza [Winkreative impeccable](https://www.winkreative.com/) hub Lufthansa.

* Nordic hub remarkable
* Boeing 787 bulletin
* Washlet sophisticated

Espresso cosy iconic charming Singapore craftsmanship. Porter airport Boeing 787 Washlet bespoke Nordic K-pop intricate Ginza. Singapore liveable sharp smart bespoke finest conversation Gaggenau Asia-Pacific.
                            """,
                            className="is-size-4 content",
                        ),
                        html.Br(),
                        dcc.Markdown(
                            """
*Paragraph text*, `is-size-6`.  **Sharp craftsmanship** sleepy, bureaux intricate cosy [Lufthansa Scandinavian exquisite](https://liveandletsfly.boardingarea.com/2018/06/28/lufthansa-first-class-houston/).


* Flat white perfect,
* Culpa handsome tote bag,
* Occaecat Scandinavian Shinkansen,


Espresso sharp iconic consectetur wardrobe, charming delightful ut eiusmod Comme des Garçons nisi conversation exercitation laboris Muji. Intricate finest dolor, Baggu liveable dolore id Melbourne Fast Lane Singapore. Lufthansa ut Shinkansen liveable.
""",
                            className="is-size-6 content",
                        ),
                    ],
                ),
            ],
        ),
    ],
)

footer = html.Footer(
    className="footer",
    children=[
        html.Div(
            className="content has-text-centered",
            children=[
                dcc.Markdown(
                    """
This is a page footer, where we'd put legal notes and other items.
                    """
                )
            ],
        )
    ],
)

app.layout = html.Div(
    className="container",
    children=[navbar, typography_section, html.Hr(), form_elements_section, footer],
)

if __name__ == "__main__":
    application.run(debug=False, port=8080)
