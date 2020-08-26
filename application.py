# pylint: disable=C0103,E0401
"""
Template for SNAP Dash apps.
"""

import os
import dash
from dash.dependencies import Input, Output
import luts
from gui import layout, path_prefix

# We set the requests_pathname_prefix to enable
# custom URLs.
# https://community.plot.ly/t/dash-error-loading-layout/8139/6
app = dash.Dash(__name__, requests_pathname_prefix=path_prefix)

# AWS Elastic Beanstalk looks for application by default,
# if this variable (application) isn't set you will get a WSGI error.
application = app.server
app.index_string = luts.index_string
app.title = luts.title
app.layout = layout

if __name__ == "__main__":
    application.run(debug=os.environ["FLASK_DEBUG"], port=8080)
