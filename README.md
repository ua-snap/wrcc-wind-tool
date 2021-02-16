# WRCC Wind Tool

Active development!

## Structure

 * `application.py` contains the main app loop code.
 * `gui.py` has most user interface elements.
 * `luts.py` has shared code & lookup tables and other configuration.
 * `assets/` has images and CSS (uses [Bulma](https://bulma.io))
 * `data/` has the preprocessed data for the app. These are tracked in git for inclusion when deploying to EB.

## Local development

Run via 

```
pipenv install
export FLASK_APP=application.py
export FLASK_DEBUG=1
pipenv run flask run
```

The project is run through Flask and will be available at [http://localhost:5000](http://localhost:5000).

## Deploying to AWS Elastic Beanstalk:

Apps run via WSGI containers on AWS.

Before deploying, make sure and run `pipenv run pip freeze > requirements.txt` to lock current versions of everything.

```
eb init
eb deploy
```

The following env vars need to be set:

 * `REQUESTS_PATHNAME_PREFIX` - URL fragment so requests are routed properly. This looks like: /tools/<name-of-tool>. Default is /
 * `GTAG_ID` - Google Tag Manager ID
