"""Pre-process station data for app ingest"""

# hacky, done to alllow import from luts.py in app dir
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# pylint: disable=all
import argparse, math, os, time
import numpy as np
import pandas as pd
from datetime import datetime
from luts import speed_ranges, decades
from multiprocessing import Pool
from pathlib import Path


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
    bin_list = [list(range(5, 356, 10)), list(np.arange(22.5, 338, 45))]
    bname_list = [list(range(1, 36)), list(np.arange(4.5, 32, 4.5))]

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
                            bucket_info["range"][0],
                            bucket_info["range"][1],
                            inclusive=True,
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
                        "month": station["month"].iloc[0],
                        "coarse": coarse,
                    },
                    ignore_index=True,
                )

    accumulator = accumulator.astype(
        {"direction_class": np.float32, "count": np.int32, "frequency": np.float32,}
    )

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
    print("Preprocessing wind rose frequency counts.")
    tic = time.perf_counter()

    # drop gusts column, discard obs with NaN in direction or speed
    stations = stations.drop(columns="gust_mph").dropna()

    # first process roses for all available stations - that is, stations
    # with data at least as old as 2010-01-01
    min_ts = stations.groupby("sid")["ts"].min()
    keep_sids = min_ts[min_ts < pd.to_datetime("2010-06-01")].index.values
    # stations to be used in the summary
    summary_stations = stations[stations["sid"].isin(keep_sids)].copy()
    # Set the decade column to "none" to indicate these are for all available data
    summary_stations["decade"] = "none"
    # set month column to 0 for annual rose data
    summary_stations["month"] = 0
    # drop calms for chunking to rose
    summary_stations = summary_stations[summary_stations["wd"] != 0].reset_index(drop=True)

    # create summary rose data
    # break out into df by station for multithreading
    station_dfs = [df for sid, df in summary_stations.groupby("sid")]

    with Pool(ncpus) as pool:
        summary_roses = pd.concat(pool.map(chunk_to_rose, station_dfs))

    print(f"Summary roses done, {round(time.perf_counter() - tic, 2)}s.")
    tic = time.perf_counter()

    # now assign actual month, break up by month and station and re-process
    summary_stations["month"] = summary_stations["ts"].dt.month
    station_dfs = [df for items, df in summary_stations.groupby(["sid", "month"])]

    with Pool(ncpus) as pool:
        monthly_roses = pd.concat(pool.map(chunk_to_rose, station_dfs))

    print(f"Monthly summary roses done, {round(time.perf_counter() - tic, 2)}s.")
    tic = time.perf_counter()

    # now focus on rose data for stations that will allow wind rose comparison
    # filter out stations where first obs is more recent than 1991-01-01
    keep_sids = min_ts[min_ts < pd.to_datetime("1991-01-01")].index.values
    stations = stations[stations["sid"].isin(keep_sids)]

    # df = df[df["ws"] != 0] # ignoring this shouldn't matter, all zero wind
    # speeds should have zero for direction
    # proc_cols = ["sid", "direction_class", "speed_range", "count"]
    # rose_data = pd.DataFrame(columns=proc_cols)

    # break out into df by station for multithreading
    # add 0 for month column first
    stations["month"] = 0
    station_dfs = [df for sid, df in stations.groupby("sid")]

    # first, subset this list to stations where there is at least sufficient data for
    # the 90's (this is assuming that if it's there for the 90's,
    # it's there for the 2010's!)
    with Pool(ncpus) as pool:
        keep_list = pool.map(check_sufficient_data, station_dfs)

    # Remove the stations lacking sufficient 90s data
    station_dfs = [df for df, keep in zip(station_dfs, keep_list) if keep]

    # break worthy stations up by decade
    # pass "False" for prelim arg to avoid filtering on 1990's
    # check again for sufficient data, for each decade
    station_dfs = [
        (decade_df, False)
        for station_df in station_dfs
        for decade, decade_df in station_df.groupby("decade")
    ]
    with Pool(ncpus) as pool:
        keep_list = pool.starmap(check_sufficient_data, station_dfs)

    # again, remove station / decade combos lacking sufficient data
    station_dfs = [df[0] for df, keep in zip(station_dfs, keep_list) if keep]

    # remaining station / decades have sufficient data for chunking to rose
    # filter out calms
    station_dfs = [df[df["wd"] != 0].reset_index(drop=True) for df in station_dfs]
    with Pool(ncpus) as pool:
        compare_roses = pd.concat(pool.map(chunk_to_rose, station_dfs))

    roses = pd.concat([summary_roses, monthly_roses, compare_roses])

    # concat and pickle it
    roses.to_pickle(roses_fp)

    print(f"Comparison roses done, {round(time.perf_counter() - tic, 2)}s.")
    print(f"Preprocessed data for wind roses written to {roses_fp}")

    return roses


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

    # first, process calms for all available stations
    # Create temporary structure which holds
    # total wind counts and counts where calm to compute
    # % of calm measurements.
    calms = stations.groupby("sid").size().reset_index()
    # keep rows where speed == 0 (calm)
    d = stations[(stations["ws"] == 0)]
    d = d.groupby("sid").size().reset_index()
    calms = calms.merge(d, on="sid")
    calms.columns = ["sid", "total", "calm"]
    calms = calms.assign(percent=round(calms["calm"] / calms["total"], 3) * 100)
    calms["decade"] = "none"
    calms["month"] = 0
    # re-order for concat below
    calms = calms[["sid", "month", "decade", "total", "calm", "percent"]]

    # next, process calms for all months for the same stations above
    stations["month"] = stations["ts"].dt.month
    monthly_calms = stations.groupby(["sid", "month"]).size().reset_index()

    # keep rows where speed == 0 (calm)
    d = stations[(stations["ws"] == 0)]
    d = d.groupby(["sid", "month"]).size().reset_index()
    monthly_calms = monthly_calms.merge(d, on=["sid", "month"])
    monthly_calms.columns = ["sid", "month", "total", "calm"]
    monthly_calms = monthly_calms.assign(percent=round(monthly_calms["calm"] / monthly_calms["total"], 3) * 100)
    monthly_calms["decade"] = "none"
    # re-order for concat below
    monthly_calms = monthly_calms[["sid", "month", "decade", "total", "calm", "percent"]]

    # repeat for comparison rose calms (include decade grouping)
    # filter stations to those with comparison rose data
    compare_sids = roses.loc[roses["decade"] == "2010-2019"]["sid"].unique()
    stations = stations[stations["sid"].isin(compare_sids)]
    compare_calms = stations.groupby(["sid", "decade"]).size().reset_index()
    # keep rows where speed == 0 (calm)
    compare_d = stations[(stations["ws"] == 0)]
    compare_d = compare_d.groupby(["sid", "decade"]).size().reset_index()
    compare_calms = compare_calms.merge(compare_d, on=["sid", "decade"])
    compare_calms.columns = ["sid", "decade", "total", "calm"]
    compare_calms = compare_calms.assign(
        percent=round(compare_calms["calm"] / compare_calms["total"], 3) * 100
    )
    compare_calms["month"] = 0
    compare_calms = compare_calms[["sid", "month", "decade", "total", "calm", "percent"]]

    # concat the two calms data sources together
    calms = pd.concat([calms, monthly_calms, compare_calms])

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


def process_wep(stations, mean_wep_fp):
    """Process wind energy potential"""
    # print(f"Preprocessing weind energy potential using {ncpus} cores", end="...")
    print(f"Preprocessing wind energy potential", end="...")
    tic = time.perf_counter()

    # drop gusts column, discard obs with NaN in direction or speed
    stations = stations.drop(columns="gust_mph").dropna()
    # filter out stations where first obs is younger than 2015-01-01
    min_ts = stations.groupby("sid")["ts"].min()
    keep_sids = min_ts[min_ts < pd.to_datetime("2015-01-01")].index.values

    stations["month"] = stations["ts"].dt.month
    stations["year"] = stations["ts"].dt.year

    rho = 1.23
    stations["wep"] = 0.5 * rho * (stations["ws"].astype(np.float32) / 2.237) ** 3
    wep = stations.drop(columns=["ws", "wd"])
    mean_wep = wep.groupby(["sid", "year", "month"]).mean().reset_index()
    mean_wep["wep"] = np.round(mean_wep["wep"])
    mean_wep = mean_wep.astype({"year": "int16", "month": "int16"})

    mean_wep.to_pickle(mean_wep_fp)

    print(f"done, {round(time.perf_counter() - tic, 2)}s")
    print(f"Wind energy potential box plot data saved to {mean_wep_fp}")

    return mean_wep


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
    ),
    args = parser.parse_args()
    ncpus = args.ncpus
    do_roses = args.roses
    do_calms = args.calms
    do_crosswinds = args.crosswinds
    do_wep = args.wep

    base_dir = Path(os.getenv("BASE_DIR"))

    # gather station data into single file
    print("Reading data")
    stations = pd.read_pickle(base_dir.joinpath("stations.pickle"))

    roses_fp = "data/roses.pickle"
    if do_roses:
        roses = process_roses(stations, ncpus, roses_fp)

    if do_calms:
        calms_fp = "data/calms.pickle"
        if not do_roses:
            roses = pd.read_pickle(roses_fp)
        calms = process_calms(stations, roses, calms_fp)

    if do_crosswinds:
        exceedance_fp = "data/crosswind_exceedance.pickle"
        crosswinds = process_crosswinds(stations, ncpus, exceedance_fp)

    if do_wep:
        wep_quantiles_fp = "data/mean_wep.pickle"
        wep = process_wep(stations, wep_quantiles_fp)
