# WRCC Wind Tool

This is an aviation-centric webapp that allows exploration of wind data collected at airports in Alaska. It was created by SNAP with the support of the [Western Regional Climate Center](https://wrcc.dri.edu/). 

This repo contains everything needed to create the app, including fetching and processing the data (this will be updated with link to data when available) and running the application.

## Structure
 
 * `ancillary/` has jupyter notebooks for tracking quality control and decisions about which data to use. 
 * `assets/` has images and CSS (uses [Bulma](https://bulma.io))
 * `data/` has the preprocessed data for the app. These are tracked in git for inclusion when deploying to EB.
 * `pipeline/` contains the scripts to fetch and process the data for app ingest.
 * `application.py` contains the main app loop code.
 * `gui.py` has most user interface elements.
 * `luts.py` has shared code & lookup tables and other configuration.

## Pipeline

This section outlines the data processing pipeline. Functionality to run the entire pipeline with a single command is not yet available. 

Set the `BASE_DIR` environmental variable to the path of the location you would like to download files to. To run the pipeline, use `pipenv run python pipeline/<pipeline_script.py>`, substituting `pipeline_script.py` with each of the following scripts in the specified order:

1. `download_iem.py`: downloads wind data from the AK_ASOS network on the [Iowa Environmental Mesonet](https://mesonet.agron.iastate.edu/request/download.phtml).
2. `scrape_meta.py`: scrapes metadata for the airports from [AirNav.com](https://www.airnav.com/).
3. `render.py -f ancillary/raw_qc.ipynb -o ancillary`: executes the jupyter notebook for initial QC investigation of raw data. * May require extra steps to run.
4. `process_raw.py`: Process the raw data into a pickeld file of all station data.
5. `preprocess.py -n <number of cores> -rcxw`: preprocess the data for app ingest. Creates the remaining files tracked in `data/`.
6. `prep_ckan.py -n <number of cores>`: prepares the cleaned and adjusted wind data for distribution on SNAP's CKAN. There is no script to facilitate transfer of these data, instead this was designed to be done on the same filesystem as the ultimate target directory and placed with `mv`. 

* Namely, adding the pipenv python install as a kernel for jupyter, as has been done on the development machine. Not doing this is untested. 

## Running the app

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
