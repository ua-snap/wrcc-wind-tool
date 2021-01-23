"""Process raw IEM data, output single optimized pickle file"""

# USE THIS SCRIPT TO DO QC DATA PROCESSING

import argparse, glob, os, time
import numpy as np
import pandas as pd
from luts import decades
from multiprocessing import Pool
from pathlib import Path


def filter_spikes(station, xname="sped", tname="valid", delta=30):
    """Remove observations that look like erroneous wind speed spikes"""
    # remove the subsequent observations after spikes
    # allows for multiple sequential spikes 
    # (e.g. up-down-up-down... unlikely but possible?)
    def remove_subsequent_idx(idx):
        k = idx[0]
        r = []
        for i in np.arange(idx[1:].shape[0]):
            if (idx[i + 1] - k) == 1:
                r.append(i + 1)
            k = idx[i + 1]
        
        return idx[np.array(r)]
    
    # speed deltas
    xd = abs(station[xname].values[1:] - station[xname].values[:-1])
    # time deltas
    td = (station[tname].values[1:] - station[tname].values[:-1]) / (10 ** 9 * 3600)
    # potential spikes
    pidx = np.where(xd > delta)[0]
    # true spike indices (time difference of two hours or less)
    try:
        sidx = remove_subsequent_idx(pidx[td[pidx].astype(float) <= 2])
    except IndexError:
        sidx = pidx
    spikes = pd.DataFrame(station.take(sidx))
    
    return station.drop(station.index[sidx]), spikes


def filter_to_hour(station):
    """Aggregate observations to nearest hour"""
    # METAR reports are typically recorded close to a clock hour, either
    # on the hour, or something like 1:07, or 12:55, etc. Instead of aggregating
    # multiple observations, in this case other SPECIals recorded, just take 
    # the observation nearest to the hour.
    # (this represents the most likely "routine" METAR record), 
    # and could help avoid potential sampling bias from SPECIals 
    # done by finding record with minimum timedelta from hour
    station["ts"] = station["valid"].dt.round("H")
    station["delta_dt"] = abs((station["ts"] - station["valid"]))
    min_dt_station = (
        station.groupby("ts").agg(min_delta_dt=pd.NamedAgg("delta_dt", "min")).reset_index()
    )

    station = pd.merge(station, min_dt_station)
    station["keep"] = station["delta_dt"] == station["min_delta_dt"]
    # filter to "keep" values and drop duplicates
    # (duplicates note: in some cases, a SPECI observation was made the exact number of
    # minutes after the hour that the routine METAR was made before the
    # hour. In this case, just take whichever occured first)
    station = station[station["keep"]].drop_duplicates(subset="ts", keep="first")

    # don't need columns used to filter to hourly obs
    return station.drop(columns=["valid", "delta_dt", "min_delta_dt", "keep"])


def read_and_process_csv(fp):
    """pandas.read_csv() wrapper for reading via Pool(), 
    and filter to hourly obs, removing erroneous data

    Args:
        fp (PosixPath): Path object to 

    Returns:
        pandas.DataFrame

    """
    # read csv data, set types
    dtype_dict = {key: np.float16 for key in ["drct", "sped", "gust_mph"]}
    station = pd.read_csv(fp, header=0, na_values="M", dtype=dtype_dict)
    station["valid"] = pd.to_datetime(station["valid"].values)

    # remove observations with wind speeds in excess of 100 mph
    station = station[station["sped"] < 100]
    # filter out spikes of 30mph or greater
    station, spikes = filter_spikes(station)

    # Filter to hourly obs closest to "on the hour"
    station = filter_to_hour(station)

    # handle observations with erroneous combination of direction/speed
    # The case where speed is 0 and direction is not is and invalid measurement.
    # Not common (<600 for entire dataset), and there is no way to know whether the error was
    # in speed or direction measurement. Drop them.
    station = station.drop(
        station[(station["sped"] == 0) & (~np.isnan(station["drct"])) & (station["drct"] != 0)].index
    )
    # As of now, not handling cases where speed is nonzero and direction is zero.
    # There are 100 times as many of these as the first case. Instead of throwing these away,
    # leave as is - it appears likely that these are observations
    # of light and variable wind, as seen in METARs e.g. for PAEI on 2011-10-17
    # stations.loc[(stations.ws != 0) & (~np.isnan(stations.ws)) & (stations.wd == 0), "drct"] = np.nan

    # Different stations have different minimum values recorded!
    # For ASOS, winds measured at 2 knots or less should be reported calm
    # this isn't always the case! for PFYU, wind speeds of 1 knot are recorded.
    # standardize by changing winds slower than 2.3 mph to calm (0 ws, 0 wd).
    station.loc[station["sped"] <= 2.3, ["sped", "drct"]] = 0

    # set negative wind gusts to NaN
    station.loc[station["gust_mph"] < 0, "gust_mph"] = np.nan

    # add decade column for aggregating
    for dyear in decades:
        dyears = list(range(dyear, dyear + 10))
        station.loc[station["ts"].dt.year.isin(dyears), "decade"] = decades[dyear]

    return (station, spikes)


def run_read_and_process(fps, ncpus):
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
        out = pool.map(read_and_process_csv, fps)

    stations = pd.concat([out_tuple[0] for out_tuple in out], ignore_index=True)
    spikes = pd.concat([out_tuple[1] for out_tuple in out], ignore_index=True)

    print(f"done, {round(time.perf_counter() - tic, 2)}s")

    return stations.rename(columns={"station": "sid", "sped": "ws", "drct": "wd"}), spikes


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

    out_fp = base_dir.joinpath("stations.pickle")
    spikes_fp = base_dir.joinpath("raw_spikes.pickle")

    # read data
    stations, spikes = run_read_and_process(raw_fps, ncpus)
    # discard stations with

    print(f"Writing processed data to {out_fp}")
    print(f"Writing filtered spikes data to {spikes_fp}", end="...")
    tic = time.perf_counter()
    stations.to_pickle(out_fp)
    spikes.to_pickle(spikes_fp)
    print(f"done, {round(time.perf_counter() - tic, 2)}s")

    return None


if __name__ == "__main__":
    main()
