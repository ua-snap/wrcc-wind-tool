# pylint: disable=C0103,C0301,E0401
"""Prepare the cleaned station data for distribution and hosting"""

import argparse
import os
import time
from multiprocessing import Pool
from pathlib import Path
import pandas as pd


def prep_write_station(station, out_dir):
    """Write a station's wind data to CSV in ckan_dir"""
    # set records with wind direction == 0 to 360
    station.loc[(station["wd"] == 0) & (station["ws"] != 0), "wd"] = 360
    sid = station["sid"].iloc[0]
    out_fp = f"alaska_airports_hourly_winds_{sid}.csv"
    station.drop(columns="sid").to_csv(
        out_dir.joinpath(out_fp), float_format="%.2f", index=False
    )

    return out_fp


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prep the cleaned station data for hosting"
    )
    parser.add_argument(
        "-n",
        "--ncpus",
        action="store",
        dest="ncpus",
        type=int,
        help="Number of cores to use with multiprocessing",
        default=8,
    )
    args = parser.parse_args()
    ncpus = args.ncpus

    base_dir = Path(os.getenv("BASE_DIR"))
    ckan_dir = base_dir.joinpath("ckan_data_package")
    ckan_dir.mkdir(exist_ok=True)

    print("Reading cleaned station data", end="...")
    stations = pd.read_pickle(base_dir.joinpath("stations.pickle"))
    print("done")
    print(
        f"Writing prepped hourly wind data to individual CSV files using {ncpus}",
        end="...",
    )
    tic = time.perf_counter()

    stations = stations[["sid", "ts", "ws_adj", "wd"]].rename(columns={"ws_adj": "ws"})
    # filter to stations that are used in the app by removing those not in roses.pickle
    roses = pd.read_pickle("data/roses.pickle")
    keep_sids = roses["sid"].unique()
    stations = [
        (df, ckan_dir) for sid, df in stations.groupby("sid") if sid in keep_sids
    ]

    with Pool(ncpus) as pool:
        _ = pool.starmap(prep_write_station, stations)

    print(f"Done, {round(time.perf_counter() - tic, 2)}s")
