"""Scrape the FAA identifier and other info from IEM and AirNAv

from AirNav.com based on IEM ASOS identifier

This method seems more straightforward than downloading data from FAA, 
could not find FAA resource making use of ICAO identifiers.

Writes a complete file of station info as "station_info.csv"
"""

import argparse, os, requests, time
import pandas as pd
from multiprocessing import Pool
from pathlib import Path


def scrape_asos_meta():
    """Get the meta data for ASOS stations provided by IEM

    Returns:
        pandas.DataFrame of station meta data on AK_ASOS network
    """
    uri = "https://mesonet.agron.iastate.edu/sites/networks.php?network=AK_ASOS&format=csv&nohtml=on"

    r = requests.get(uri)

    meta_list = [t.split(",") for t in r.content.decode()[:-2].split("\n")]

    meta = pd.DataFrame(meta_list)
    meta.columns = meta.iloc[0]
    meta = meta.drop(0)
    meta = meta.drop(columns="iem_network")

    return meta


def parse_runway_info(rw_info):
    """Extract runway names and info from Runway Information section"""

    def parse_rw(rw):
        rw_name = rw.split("</H4>")[0]
        try:
            heading = int(rw.split("magnetic, ")[1][:3])
        except IndexError:
            heading = None

        return {"rw_name": rw_name, "rw_heading": heading}

    return pd.DataFrame([parse_rw(rw) for rw in rw_info])


def scrape_airnav(uri):
    """Scrape relevant data from AirNav.com for a particular ICAO identifier"""

    print(f"Requesting {uri}")
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36'}
    r = requests.get(uri, headers=headers)
    print(f"Initial status code {r.status_code}")
    # handle retries, return fail after 10
    # setup fail df to return
    stid = uri[-4:]
    colnames = ["rw_name", "rw_heading", "faa_id", "name", "stid"]
    fail_df = pd.DataFrame(
        {k: [v] for k, v in zip(colnames, (None, None, None, None, stid))}
    )
    if r.status_code != 200:
        wait = [0.1 * 2 ** x for x in list(range(10))]
        for t in wait:
            time.sleep(t)
            print(f"Retrying {uri}")
            r = requests.get(uri)
            if r.status_code == 200:
                break
            elif (r.status_code != 200) & (t == wait[-1]):
                print(f"Max attempts exceded for {uri}")
                return fail_df

    # get HTML as string
    page_html = r.content.decode()

    # if this fails because of index error, probably not available AirNav
    try:
        faa_id = page_html.split("FAA Identifier")[1].split("<TD>")[1][:3]
    except IndexError:
        return fail_df

    # get common name for airport
    try:
        name = page_html.split(f"{stid} - ")[1].split("<")[0]
    except:
        print(f"{stid} name not found")
        exit()
    # this gives list of strings for each runway
    rw_info = page_html.split("Runway Information</H3>\n")[1].split("<H4>")[1:]
    try:
        airport = parse_runway_info(rw_info)
    except IndexError:
        print(stid)
        exit("exit")
    airport["faa_id"] = faa_id
    airport["real_name"] = name
    airport["sid"] = stid

    print(airport)
    return airport


def run_scrape_airnav(meta, ncpus):
    """Scrape data from AirNav.com and add to meta data frame

    Returns:
        ASOS metadata with other relevant airport info included
    """
    base_uri = "https://www.airnav.com/airport/{}"

    # construct uris
    uris = [base_uri.format(stid) for stid in meta["stid"].unique()]

    print(f"Scraping AirNav.com data using {ncpus} cores", end="...")
    print(f"Scraping AirNav.com data for all stations, this could take a while", end="...")
    tic = time.perf_counter()

    # with Pool(ncpus) as pool:
        # airnav_df = pd.concat(pool.map(scrape_airnav, uris))

    airnav_df = pd.concat([scrape_airnav(uri) for uri in uris])

    print(f"done, {round(time.perf_counter() - tic, 2)}s")

    return meta.merge(airnav_df, on="stid")


def main():
    parser = argparse.ArgumentParser(
        description="Extract the date ranges of historical 8-day MODIS data"
    )
    parser.add_argument(
        "-n",
        "--ncpus",
        action="store",
        dest="ncpus",
        type=int,
        default=8,
        help="Number of cores to use with multiprocessing",
    )
    args = parser.parse_args()
    ncpus = args.ncpus

    base_dir = Path(os.getenv("BASE_DIR"))

    meta = scrape_asos_meta()
    meta = run_scrape_airnav(meta, ncpus)

    out_fp = base_dir.joinpath("airport_meta.csv")
    meta.to_csv(out_fp, index=False)

    print(f"Airport metadata saved to to {out_fp}")


if __name__ == "__main__":
    main()
