"""Process raw IEM data, output single optimized pickle file"""

# USE THIS SCRIPT TO DO QC DATA PROCESSING

import argparse, glob, os, time
import numpy as np
import pandas as pd
from multiprocessing import Pool
from pathlib import Path


def read_and_filter_csv(fp):
    """pandas.read_csv() wrapper for reading via Pool(), and filter to hourly obs

    Args:
        fp (PosixPath): Path object to 

    Returns:
        pandas.DataFrame

    """
    dtype_dict = {key: np.float16 for key in ["drct", "sped", "gust_mph"]}
    df = pd.read_csv(fp, header=0, na_values="M", dtype=dtype_dict)
    df["valid"] = pd.to_datetime(df["valid"].values)

    # handle observations with erroneous combination of direction/speed
    # The case where speed is 0 and direction is not is and invalid measurement.
    # Not common (<600 for entire dataset), and there is no way to know whether the error was
    # in speed or direction measurement. Drop them.
    df = df.drop(
        df[(df["sped"] == 0) & (~np.isnan(df["drct"])) & (df["drct"] != 0)].index
    )

    # As of now, not handling cases where speed is nonzero and direction is zero.
    # There are 100 times as many of these as the first case. Instead of throwing these away,
    # leave as is - it appears likely that these are observations
    # of light and variable wind, as seen in METARs e.g. for PAEI on 2011-10-17
    # df.loc[(df.ws != 0) & (~np.isnan(df.ws)) & (df.wd == 0), "drct"] = np.nan

    # set negative wind gusts to NaN
    df.loc[df["gust_mph"] < 0, "gust_mph"] = np.nan

    # Filter to hourly obs closest to "on the hour"
    # (this represents the most likely "routine" METAR record)
    # done by finding record with minimum timedelta from hour
    df["ts"] = df["valid"].dt.round("H")
    df["delta_dt"] = abs((df["ts"] - df["valid"]))
    min_dt_df = (
        df.groupby("ts").agg(min_delta_dt=pd.NamedAgg("delta_dt", "min")).reset_index()
    )

    df = pd.merge(df, min_dt_df)
    df["keep"] = df["delta_dt"] == df["min_delta_dt"]
    # filter to "keep" values and drop duplicates
    # (duplicates note: in some cases, a SPECI observation was made the exact number of
    # minutes after the hour that the routine METAR was made before the
    # hour.)
    df = df[df["keep"]].drop_duplicates(subset="ts", keep="first")

    # don't need columns used to filter to hourly obs
    return df.drop(columns=["valid", "delta_dt", "min_delta_dt", "keep"])


def run_read_data(fps, ncpus):
    """Read the raw data files

    Args:
        fps (list): list of filepaths of IEM ASOS data

    Returns:
        pandas.DataFrame of all individual sites
    
    Notes:
        The timestamp in the returned data is the NEAREST HOUR of the obs
    """
    print(f"Reading data using {ncpus} cores", end="...")
    tic = time.perf_counter()

    with Pool(8) as pool:
        df = pd.concat(pool.map(read_and_filter_csv, fps), ignore_index=True)

    print(f"done, {round(time.perf_counter() - tic, 2)}s")

    return df.rename(columns={"station": "sid", "sped": "ws", "drct": "wd"})


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
        help="Number of cores to use with multiprocessing",
        default=8,
    )
    args = parser.parse_args()
    ncpus = args.ncpus

    base_dir = Path(os.getenv("BASE_DIR"))
    raw_dir = base_dir.joinpath("raw/iem")
    raw_fps = list(raw_dir.glob("*"))

    # CHANGE NAME
    out_fp = Path("data/stations_test.pickle")

    # read data
    df = run_read_data(raw_fps, ncpus)
    # discard stations with

    print(f"Writing processed data to {out_fp}", end="...")
    tic = time.perf_counter()
    df.to_pickle(out_fp)
    print(f"done, {round(time.perf_counter() - tic, 2)}s")

    return None


if __name__ == "__main__":
    main()
