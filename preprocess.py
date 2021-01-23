"""Pre-process station data for app ingest"""

# pylint: disable=all
import argparse, math, os, time
import dask.dataframe as dd
import numpy as np
import pandas as pd
from datetime import datetime
from luts import speed_ranges, decades
from multiprocessing import Pool
from pathlib import Path


def process_stations(stations, prepped_fp):
    """Prepare data for more specific graphic pre-processing.  

    Args:
        df (DataFrame): Data frame of all hourly raw data from stations

    Returns:
        Stations DataFrame prepared for further preprocessing

    """
    # add decade column for aggregating
    for dyear in decades:
        dyears = list(range(dyear, dyear + 10))
        stations.loc[stations["ts"].dt.year.isin(dyears), "decade"] = decades[dyear]

    # Not sure what else to include in this function as of now.
    stations.to_pickle(prepped_fp)

    return stations


def check_sufficient_data(station, prelim=True, r1=0.25, r2=0.75):
    """Check presence of sufficient hourly time series data 
    against some thresholds

    Args:
        station (pandas.DataFrame): Station DataFrame, either all data (if prelim=True) or 
        subsetted to decade to test
        r1 (float): daily observation threshold (proportion)
        r2 (float): total sufficient day threshold (proportion)
    
    Returns:
        True if daily observation threshold is met for total sufficient day threshold
        proportion of days; otherwise, False

    """
    # This is done to test if a station should even be considered for the 
    # comparison wind rose processing
    if prelim:
        station = station[station["decade"] == "1990-1999"]
    # if no 90s data, return False to ignore
    try:
        dyear = int(str(station["ts"].dt.year.values[0])[:3] + "0")
    except IndexError:
        return False

    ndays = (
        pd.to_datetime(f"{dyear + 9}-12-31") - pd.to_datetime(f"{dyear}-01-01")
    ).days

    return (station.ts.dt.date.value_counts() / 24 > r1).sum() / ndays > r2


def chunk_to_rose(station):
    """
    Builds data suitable for Plotly's wind roses from
    a subset of data.

    Given a subset of data, group by direction and speed.
    Return accumulator of whatever the results of the
    incoming chunk are.
    """
    # if coarse, bin into 8 categories, 36 otherwise
    bin_list = [list(range(5, 356, 10)), list(np.arange(22.5, 337.5, 45))]
    bname_list = [list(range(1, 36)), list(np.arange(4.5, 31.5, 4.5))]

    # Accumulator dataframe.
    proc_cols = [
        "sid",
        "direction_class",
        "speed_range",
        "count",
        "frequency",
        "decade",
        "coarse",
    ]
    accumulator = pd.DataFrame(columns=proc_cols)

    for bins, bin_names, coarse in zip(bin_list, bname_list, [False, True]):
        # Assign directions to bins.
        # We'll use the exceptional 'NaN' class to represent
        # 355º - 5º, which would otherwise be annoying.
        # Assign 0 to that direction class.
        ds = pd.cut(station["wd"], bins, labels=bin_names)
        station = station.assign(direction_class=ds.cat.add_categories("0").fillna("0"))

        # First compute yearly data.
        # For each direction class...
        directions = station.groupby(["direction_class"])
        for direction, d_group in directions:

            # For each wind speed range bucket...
            for bucket, bucket_info in speed_ranges.items():
                d = d_group.loc[
                    (
                        station["ws"].between(
                            bucket_info["range"][0], bucket_info["range"][1], inclusive=True
                        )
                        == True
                    )
                ]
                count = len(d.index)
                full_count = len(station.index)
                frequency = 0
                if full_count > 0:
                    frequency = round(((count / full_count) * 100), 2)

                accumulator = accumulator.append(
                    {
                        "sid": station["sid"].values[0],
                        "direction_class": direction,
                        "speed_range": bucket,
                        "count": count,
                        "frequency": frequency,
                        "decade": station["decade"].iloc[0],
                        "coarse": coarse,
                    },
                    ignore_index=True,
                )

    accumulator = accumulator.astype({
        "direction_class": np.float32,
        "count": np.int32,
        "frequency": np.float32,
    })

    return accumulator


def process_roses(stations, ncpus, roses_fp):
    """
    For each station we need one trace for each direction.

    Each direction has a data series containing the frequency
    of winds within a certain range.

    Columns:

    sid - stationid
    direction_class - number between 0 and 35.  0 represents
       directions between 360-005º (north), and so forth by 10 degree
       intervals.
    speed_range - text fragment from luts.py for the speed class
    month - 0 for year, 1-12 for month

    Args:
        stations (pandas.Dataframe): Processed raw station data

    Returns:
        filepath where pre-processed wind rose data was saved

    """
    print("Preprocessing wind rose frequency counts", end="...")
    tic = time.perf_counter()

    # drop gusts column, discard obs with NaN in direction or speed
    stations = stations.drop(columns="gust_mph").dropna()
    # filter out stations where first obs is younger than 1991-01-01
    min_ts = stations.groupby("sid")["ts"].min()
    keep_sids = min_ts[min_ts < pd.to_datetime("1991-01-01")].index.values
    stations = stations[stations["sid"].isin(keep_sids)]

    # df = df[df["ws"] != 0] # ignoring this shouldn't matter, all zero wind
    # speeds should have zero for direction
    # proc_cols = ["sid", "direction_class", "speed_range", "count"]
    # rose_data = pd.DataFrame(columns=proc_cols)

    # break out into df by station for multithreading
    station_dfs = [df for sid, df in stations.groupby("sid")]

    # first, subset this list to stations where there is at least sufficient data for
    # the 90's (this is assuming that if it's there for the 90's,
    # it's there for the 2010's!)
    with Pool(ncpus) as pool:
        keep_list = pool.map(check_sufficient_data, station_dfs)
    # Remove the stations lacking sufficient 90s data
    station_dfs = [df for df, keep in zip(station_dfs, keep_list) if keep]

    # suff_sids = [df.sid.unique()[0] for df in station_dfs]
    # print(f"\nsufficient 90s data: {suff_sids} \n")
    # print(len(suff_sids))

    # break worthy stations up by decade
    # pass "False" for prelim arg to avoid filtering on 1990's
    station_dfs = [
        (decade_df, False)
        for station_df in station_dfs
        for decade, decade_df in station_df.groupby("decade")
    ]
    with Pool(ncpus) as pool:
        keep_list = pool.starmap(check_sufficient_data, station_dfs)
    # again, remove station / decade combos lacking sufficient data
    station_dfs = [df[0] for df, keep in zip(station_dfs, keep_list) if keep]

    # suff_sids = [df.sid.unique()[0] for df in station_dfs]
    # suff_sids2 = []
    # for sid in suff_sids:
    #     if sid not in suff_sids2:
    #         suff_sids2.append(sid)
    # print(f"sufficient overall data: {suff_sids2} \n")
    # print(len(suff_sids2))

    # remaining station / decades have sufficient data for chunking to rose
    # filter out calms
    station_dfs = [df[df["wd"] != 0].reset_index(drop=True) for df in station_dfs]
    with Pool(ncpus) as pool:
        rose_dfs = pool.map(chunk_to_rose, station_dfs)

    roses = pd.concat(rose_dfs)
    # concat and pickle it
    roses.to_pickle(roses_fp)

    print(f"done, {round(time.perf_counter() - tic, 2)}s")
    print(f"Preprocessed data for wind roses written to {roses_fp}")

    return roses_fp


def process_calms(stations, roses, calms_fp):
    """
    For each station/year/month, generate a count
    of # of calm measurements.

    makes use of roses df for determining which stations need calms
    """
    print("Generating calm counts", end="...")
    tic = time.perf_counter()

    # filter stations to only those in roses
    stations = stations[stations["sid"].isin(roses["sid"].unique())]
    # drop gusts column, discard obs with NaN in direction or speed
    stations = stations.drop(columns="gust_mph").dropna()
    # Create temporary structure which holds
    # total wind counts and counts where calm to compute
    # % of calm measurements.
    calms = stations.groupby(["sid", "decade"]).size().reset_index()
    # keep rows where speed == 0 (calm)
    d = stations[(stations["ws"] == 0)]
    d = d.groupby(["sid", "decade"]).size().reset_index()
    calms = calms.merge(d, on=["sid", "decade"])
    calms.columns = ["sid", "decade", "total", "calm"]
    calms = calms.assign(percent=round(calms["calm"] / calms["total"], 3) * 100)
    # remove remaining decades not present in station roses data
    sid_decades = roses["sid"] + roses["decade"]
    calms = calms[(calms["sid"] + calms["decade"]).isin(sid_decades)]
    # pickle it
    calms.to_pickle(calms_fp)

    print(f"done, {round(time.perf_counter() - tic, 2)}s")
    print(f"Calms data for wind roses saved to {calms_fp}")

    return calms


def crosswind_component(ws, wd, d=0):
    """Compute crosswind component(s), ws and wd may be arrays"""
    angles = ((wd - d) + 180) % 360 - 180
    return np.round(
        np.array([abs(math.sin(math.radians(a)) * w) for a, w in zip(angles, ws)]), 2
    )


def exceedance_frequencies(winds, d, thresholds):
    """Compute exceedance frequencies for given thresholds"""
    crosswinds = crosswind_component(winds["ws"], winds["wd"], d)
    n = crosswinds.shape[0]
    return [
        round((crosswinds > threshold).sum() / n, 4) * 100 for threshold in thresholds
    ]


def compute_exceedance(station, thresholds):
    """Compute exceedance frequencies for single station and set of thresholds"""
    directions = np.arange(0, 180, 10)
    exceedance = [
        exceedance_frequencies(station[["ws", "wd"]], d, thresholds) for d in directions
    ]
    exceedance = [
        f for frequencies in exceedance for f in frequencies
    ]  # unpack smal lists
    return pd.DataFrame(
        {
            "sid": station["sid"].values[0],
            "direction": np.repeat(directions, thresholds.shape[0]),
            "threshold": np.tile(thresholds, directions.shape[0]),
            "exceedance": exceedance,
        }
    )


def process_crosswinds(stations, ncpus, exceedance_fp):
    """compute crosswind component frequencies"""
    print("Preprocessing allowable crosswind exceedance", end="...")
    tic = time.perf_counter()

    # drop gusts column, discard obs with NaN in direction or speed
    stations = stations.drop(columns="gust_mph").dropna()
    # filter out stations where first obs is younger than 2015-01-01
    min_ts = stations.groupby("sid")["ts"].min()
    keep_sids = min_ts[min_ts < pd.to_datetime("2015-01-01")].index.values
    stations = stations[stations["sid"].isin(keep_sids)]

    thresholds = np.round(np.array([10.5, 13, 16]) * 1.15078, 2)
    # compute exceedances in parallel for three thresholds
    args = [(df, thresholds) for sid, df in stations.groupby("sid")]
    with Pool(ncpus) as pool:
        exceedance_dfs = pool.starmap(compute_exceedance, args)

    exceedance = pd.concat(exceedance_dfs)
    exceedance.to_pickle(exceedance_fp)

    print(f"done, {round(time.perf_counter() - tic, 2)}s")
    print(f"Allowable exceedance frequencies saved to {exceedance_fp}")

    crosswinds = exceedance

    return crosswinds


def compute_wep_box(station):

    station["wep"] = np.round(station["ws"].astype(np.float32) ** 3, 2)
    # hack to create boxplots from summarized data
    quantiles = [0.05, 0.25, 0.25, 0.5, 0.75, 0.75, 0.95]
    quantiles_list = [(np.round(station.groupby(["year", "month"])["wep"].quantile(q), 2), q)
                for q in quantiles]
    quantiles_used = [q for i in quantiles_list for q in np.repeat(i[1], len(i[0].index))]
    wep_quantiles = pd.DataFrame(pd.concat([i[0] for i in quantiles_list])).reset_index()
    # add quantile value for aggregating
    wep_quantiles["quantile"] = quantiles_used
    wep_quantiles = wep_quantiles.groupby(["month", "quantile"])["wep"].mean().reset_index()
    wep_quantiles["sid"] = station["sid"].values[0]

    return wep_quantiles


def process_wep(stations, ncpus, wep_quantiles_fp):
    """Process wind energy potential"""
    print(f"Preprocessing weind energy potential using {ncpus} cores", end="...")
    tic = time.perf_counter()

    # drop gusts column, discard obs with NaN in direction or speed
    stations = stations.drop(columns="gust_mph").dropna()
    # filter out stations where first obs is younger than 2015-01-01
    min_ts = stations.groupby("sid")["ts"].min()
    keep_sids = min_ts[min_ts < pd.to_datetime("2015-01-01")].index.values

    stations["month"] = stations["ts"].dt.month
    stations["year"] = stations["ts"].dt.year

    # this may not be more efficient than the multilevel groupby, explore later
    station_dfs = [df for sid, df in stations.groupby("sid")]
    with Pool(ncpus) as pool:
        wep_quantiles = pd.concat(pool.map(compute_wep_box, station_dfs))

    wep_quantiles.to_pickle(wep_quantiles_fp)

    print(f"done, {round(time.perf_counter() - tic, 2)}s")
    print(f"Wind energy potential box plot data saved to {wep_quantiles_fp}")

    return wep_quantiles


if __name__ == "__main__":
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
    parser.add_argument(
        "-s",
        "--stations",
        action="store_true",
        dest="stations",
        help="Pre-process (gather) station data",
    )
    parser.add_argument(
        "-r",
        "--roses",
        action="store_true",
        dest="roses",
        help="Pre-process speed/direction data for wind roses",
    )
    parser.add_argument(
        "-c",
        "--calms",
        action="store_true",
        dest="calms",
        help="Pre-process calms data for wind roses",
    )
    parser.add_argument(
        "-x",
        "--crosswinds",
        action="store_true",
        dest="crosswinds",
        help="Pre-process crosswinds data",
    )
    parser.add_argument(
        "-w",
        "--wind-energy",
        action="store_true",
        dest="wep",
        help="Pre-process wind energy potential data",
    )
    args = parser.parse_args()
    ncpus = args.ncpus
    do_stations = args.stations
    do_roses = args.roses
    do_calms = args.calms
    do_crosswinds = args.crosswinds
    do_wep = args.wep

    base_dir = Path(os.getenv("BASE_DIR"))

    # gather station data into single file
    print("Reading data")
    prepped_fp = base_dir.joinpath("stations_prepped.pickle")
    if do_stations:
        # stations = process_stations("data/asos", ncpus)
        stations = pd.read_pickle(base_dir.joinpath("stations.pickle"))
        stations = process_stations(stations, prepped_fp)
    else:
        stations = pd.read_pickle(prepped_fp)

    roses_fp = base_dir.joinpath("roses.pickle")
    if do_roses:
        roses = process_roses(stations, ncpus, roses_fp)

    if do_calms:
        calms_fp = base_dir.joinpath("calms.pickle")
        if not do_roses:
            roses = pd.read_pickle(roses_fp)
        calms = process_calms(stations, roses, calms_fp)

    if do_crosswinds:
        exceedance_fp = base_dir.joinpath("crosswind_exceedance.pickle")
        crosswinds = process_crosswinds(stations, ncpus, exceedance_fp)

    if do_wep:
        wep_quantiles_fp = base_dir.joinpath("wep_box_data.pickle")
        wep = process_wep(stations, ncpus, wep_quantiles_fp)
