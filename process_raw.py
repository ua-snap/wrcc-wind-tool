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
    df.valid = pd.to_datetime(df.valid.values)

    # Filter to hourly obs closest to "on the hour"
    # (this represents the most likely "routine" METAR record)
    # done by finding record with minimum timedelta from hour
    df["valid_h"] = df.valid.dt.round("H")
    df["delta_dt"] = abs((df.valid_h - df.valid))
    min_dt_df = (
        df.groupby(["valid_h"])
        .agg(min_delta_dt=pd.NamedAgg("delta_dt", "min"))
        .reset_index()
    )

    df = pd.merge(df, min_dt_df)
    df["keep"] = df["delta_dt"] == df["min_delta_dt"]

    return df[df["keep"]].drop(columns=["valid_h", "delta_dt", "min_delta_dt"])


def run_read_data(fps, ncpus):
    """Read the raw data files

    Args:
        fps (list): list of filepaths of IEM ASOS data

    Returns:
        pandas.DataFrame of all individual sites
    
    """
    print(f"Reading data using {ncpus} cores", end="...")
    tic = time.perf_counter()

    with Pool(8) as pool:
        df = pd.concat(pool.map(read_and_filter_csv, fps), ignore_index=True)

    print(f"done, {round(time.perf_counter() - tic, 2)}s")

    return df


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
