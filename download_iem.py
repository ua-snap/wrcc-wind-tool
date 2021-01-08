"""Download ASOS wind data for preprocessing and app ingest

Currently hardwired to download all wind speed and direction data for 
all AK_ASOS network stations, for 1980-2020
"""

import os, requests, time
from multiprocessing import Pool
from pathlib import Path


BASE_URI = "http://mesonet.agron.iastate.edu/"



def get_stations(network):
    """Build a station list from a network

    Args:
        network (str): namne of IEM ASOS network to use (defaults to AK)

    Returns:
        a list of four letter ICAO station identifiers
    """
    uri = (
        f"{BASE_URI}geojson/network/{network}.geojson"
    )
    r = requests.get(uri)
    jdict = r.json()
    return  [site["properties"]["sid"] for site in jdict["features"]]


def make_uris(sids, start, end):
    """Make the URIs for all stations
    
    Args:
        sids (list): List of IACO station identifiers (str)
        start (str): Starting date in YYYY-mm-dd
        end (str): ending date in YYYY-mm-dd

    Returns:
        a list of URIs, one for each dataset
    """
    service = BASE_URI + "cgi-bin/request/asos.py?"
    # speed (mph) and direction, comma-separated (no debug header) no latlot or elev
    service += "data=sped&data=drct&tz=Etc/UTC&format=onlycomma&latlon=no&elev=no&"
    # add start and ending dates
    start, end = start.split("-"), end.split("-")
    service += f"year1={start[0]}&month1={start[1]}&day1={start[2]}&"
    service += f"year2={end[0]}&month2={end[1]}&day2={end[2]}&"

    return [service + f"station={sid}" for sid in sids]


def get_with_retry(uri, max_retry=5):
    """Wrapper for requests.get() to retry
    
    Args:
        uri (str): URI to request 
        max_retry (int): number of retries to make
    
    Returns:
        the requests response
    """
    r = requests.get(uri)
    k = 0
    while r.status_code != 200:
        if k == max_retry:
            print(f"{uri} failed.")
            break
        time.sleep(1)
        r = requests.get(uri)
        k += 1

    return r


def download_file(uri, out_fp):
    """Download a single file to a directory
    
    Args:
        uri (str): uri of query to download
        out_fp (PosixPath): PosixPath object of output filepath
    
    Returns:
        path to the downloaded file
    """

    # make filename from URI
    sid = uri.split("station=")[1][:4]
    # download and write
    r = get_with_retry(uri)
    if r.status_code == 200:
        f = open(out_fp, 'w')
        f.write(r.text)
        f.close()
        return str(out_fp)
    else:
        print(f"{uri} download failed.")


def run_download(network, start, end, out_dir, ncpus=8):
    """Download urls to an output directory in parallel
    
    Args:
        network (str): name of ASOS network on IEM (e.g. AK_ASOS)
        start (str): start date string (YYYY-mm-dd) 
        end (str): end date string (YYYY-mm-dd)
        out_dir (PosixPath): PosixPath object of directory to write files
        ncpus (int): number of cores to use in downloading files in parallel
    
    Returns:
        paths of successfully downloaded files
    """
    # construct URIs
    sids = get_stations(network)
    uris = make_uris(sids, start, end)
    # construct output filepaths
    start, end = start.replace("-", ""), end.replace("-", "")
    base_fn = f"iem_asos_hourly_winds_{{}}_{start}-{end}.csv"
    out_fps = [out_dir.joinpath(base_fn.format(sid)) for sid in sids]

    print(f"Downloading {len(uris)} files", sep="...")

    dl_args = [(uri, fp) for uri, fp in zip(uris, out_fps)]
    with Pool(ncpus) as pool:
        out_fps = pool.starmap(download_file, dl_args)
    
    print("done.")

    return out_fps


def main():
    tic = time.perf_counter()

    # hardwired download to BASE_DIR
    base_dir = Path(os.getenv("BASE_DIR"))
    out_dir = base_dir.joinpath("raw/iem")
    # ensure download dir exists
    out_dir.mkdir(parents=True, exist_ok=True)

    network = "AK_ASOS"
    start, end = "1980-01-01", "2019-12-31"

    out_fps = run_download(network, start, end, out_dir)

    print(f"Elapsed time: {round(time.perf_counter() - tic, 1)} s")
    print("Downloaded files:\n")
    _ = [print(fp) for fp in out_fps]


if __name__ == '__main__':
    main()