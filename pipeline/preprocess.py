# pylint: disable=C0103,C0301,E0401
"""Pre-process station data for app ingest"""

import argparse
import math
import os
import sys
import time
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path
from random import choice
import numpy as np
import pandas as pd
from scipy.stats import ks_2samp
# this hack is done to alllow import from luts.py in app dir
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from luts import speed_ranges, exceedance_classes


def check_sufficient_comparison_rose_data(station, r1=0.25, r2=0.75):
    """ check sufficient data for a single station, and return
    the station data for the 2010s and the oldest decade available 
    between 80s and 90s

    Args:
        station (pandas.DataFrame): Station DataFrame
        r1 (float): daily observation threshold (proportion)
        r2 (float): total sufficient day threshold (proportion)
    
    Returns:
        DataFrame of station data filtered to first and last decade 
        with sufficient data if available. Otherwise, None.

    """

    def is_sufficient(df, r1, r2):
        """Helper function to check if there sufficient sampling in a time series"""
        dyear = int(str(df["ts"].dt.year.values[0])[:3] + "0")
        ndays = (
            pd.to_datetime(f"{dyear + 9}-12-31") - pd.to_datetime(f"{dyear}-01-01")
        ).days
        return (df.ts.dt.date.value_counts() / 24 > r1).sum() / ndays > r2

    station = {decade: df for decade, df in station.groupby("decade")}
    decades = list(station.keys())
    if "2010-2019" not in decades:
        return None
    else:
        recent_data = station["2010-2019"]
        if is_sufficient(recent_data, r1, r2):
            old_decades = ["1980-1989", "1990-1999"]
            decades = [decade for decade in old_decades if decade in decades]
            for decade in decades:
                if is_sufficient(station[decade], r1, r2):
                    return pd.concat([station[decade], station["2010-2019"]])
            return None  # not sufficient data in old decade(s)
        else:
            return None  # insufficient 2010s data


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


def adjust_sampling(station):
    """Try to achieve sampling parity based on Kolomogrov-Smirnov
    test on hour-of-day sampling between two decades to be compared.
    Iteratively removes observations until sampling parity is achieved
    based on p-value of KS test. 

    Returns a DataFrame of station data with discarded observations,
    and a dataframe of those observations"""

    # do initial check on hour-of-year sampling symmetry between decades
    # determine hour of year and split up by decade
    station["hour"] = station["ts"].dt.hour
    station["hoy"] = station["hour"] + (station["ts"].dt.dayofyear - 1) * 24
    d1, d2 = [df for decade, df in station.groupby("decade")]
    pval = ks_2samp(d1["hour"], d2["hour"])[1]
    # quick check just return station if no adjustment needed
    if pval >= 0.05:
        station = station.drop(columns=["hour", "hoy"])
        return station, station[station["decade"] == "cats"]  # empty dataframe

    # Now, iteratively remove observations systematically based on hour of year
    # the code below compares the frequencies of all hour-of-year, and
    # removes 1 observation based on each round of disparities.
    # e.g., looking at the 5th hour of the year (YYYY-01-01 05:00:00),
    # if decade 1 had 3 observations and decade two had 10, randomly choose
    # one of those 10 observations from decade 2 to remove.
    # Do this for all hours and re-test until passing.

    # init empty data frame for timestamps to be removed
    rm_df = station[station["decade"] == "cats"]
    rm_df["iter"] = None  # column to store iteration number
    station = station.set_index("ts")  # set ts indes for discarding obs
    # create a data frame for filling in missing hours-of-year with zeros
    # there are 366 * 24 = 8784 hour periods in a leap year
    n = 8784
    hoy = np.arange(n)
    d1_name, d2_name = station["decade"].unique()
    hoy_df = pd.DataFrame(
        {
            "decade": np.concatenate([np.repeat(d1_name, n), np.repeat(d2_name, n)]),
            "hoy": np.tile(hoy, 2),
        }
    )
    k = 1  # iter counter
    while pval < 0.05:

        counts = (
            station.groupby(["decade", "hoy"])["sid"]
            .count()
            .reset_index()
            .rename(columns={"sid": "count"})
        )
        counts = counts.merge(hoy_df, on=["decade", "hoy"], how="outer").sort_values(
            ["decade", "hoy"]
        )
        counts.loc[np.isnan(counts["count"]), "count"] = 0
        d1_counts, d2_counts = [
            df["count"].values for decade, df in counts.groupby("decade")
        ]
        d1_prune_hoy = np.argwhere(d1_counts - d2_counts > 0).flatten()
        d2_prune_hoy = np.argwhere(d2_counts - d1_counts > 0).flatten()
        d1_station, d2_station = [df for decade, df in station.groupby("decade")]
        rm_ts = []
        for i in d1_prune_hoy:
            # random timestamp to drop
            rm_ts.append(choice(d1_station[d1_station["hoy"] == i].index))
        for i in d2_prune_hoy:
            # random timestamp to drop
            rm_ts.append(choice(d2_station[d2_station["hoy"] == i].index))

        station = pd.concat([d1_station, d2_station])
        temp_rm_df = station.loc[rm_ts].copy()
        temp_rm_df["iter"] = k
        k += 1
        rm_df = pd.concat([rm_df, temp_rm_df])
        station = station.drop(rm_ts)
        # re-test
        d1, d2 = [df for decade, df in station.groupby("decade")]
        pval = ks_2samp(d1["hour"], d2["hour"])[1]

    return station.reset_index(), rm_df


def process_roses(stations, ncpus, roses_fp, discard_obs_fp):
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
    summary_stations = summary_stations[summary_stations["wd"] != 0].reset_index(
        drop=True
    )

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

    # break out into df by station for multithreading
    # add 0 for month column first
    stations["month"] = 0
    station_dfs = [df for sid, df in stations.groupby("sid")]

    # check sufficient data for each station, and return the station date for
    # only 2010s and the oldest decade available between 80s and 90s
    with Pool(ncpus) as pool:
        compare_station_dfs = pool.map(
            check_sufficient_comparison_rose_data, station_dfs
        )

    compare_station_dfs = [df for df in compare_station_dfs if df is not None]

    # now can discard observations to achieve sampling parity between decades
    with Pool(ncpus) as pool:
        adjustment_results = pool.map(adjust_sampling, compare_station_dfs)
    # break up tuples of data/discarded data
    compare_station_dfs = [tup[0] for tup in adjustment_results]
    discarded_obs = pd.concat([tup[1] for tup in adjustment_results])

    print(
        f"Station data adjusted, chunking comparison roses, {round(time.perf_counter() - tic, 2)}s."
    )
    tic = time.perf_counter()

    # finally can chunk to rose for comparison roses - first filter out calms
    compare_station_dfs = [
        df[df["wd"] != 0].reset_index(drop=True) for df in compare_station_dfs
    ]
    # then break up by decade for chunking to rose
    compare_station_dfs = [
        df
        for station in compare_station_dfs
        for decade, df in station.groupby("decade")
    ]

    with Pool(ncpus) as pool:
        compare_roses = pd.concat(pool.map(chunk_to_rose, compare_station_dfs))

    roses = pd.concat([summary_roses, monthly_roses, compare_roses])

    # concat and pickle it
    roses.to_pickle(roses_fp)
    discarded_obs.to_csv(discard_obs_fp)

    print(f"Comparison roses done, {round(time.perf_counter() - tic, 2)}s.")
    print(f"Preprocessed data for wind roses written to {roses_fp}")
    print(
        f"Discarded observations for comparison wind roses written to {discard_obs_fp}"
    )

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
    monthly_calms = monthly_calms.assign(
        percent=round(monthly_calms["calm"] / monthly_calms["total"], 3) * 100
    )
    monthly_calms["decade"] = "none"
    # re-order for concat below
    monthly_calms = monthly_calms[
        ["sid", "month", "decade", "total", "calm", "percent"]
    ]

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
    compare_calms = compare_calms[
        ["sid", "month", "decade", "total", "calm", "percent"]
    ]

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
            "rdc_class": np.tile(exceedance_classes, directions.shape[0]),
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

    return exceedance


def process_wep(stations, mean_wep_fp):
    """Process wind energy potential"""
    print(f"Preprocessing wind energy potential", end="...")
    tic = time.perf_counter()

    # drop gusts column, discard obs with NaN in direction or speed
    stations = stations.drop(columns="gust_mph").dropna()
    # filter out stations where first obs is younger than 2015-01-01
    min_ts = stations.groupby("sid")["ts"].min()
    keep_sids = min_ts[min_ts < pd.to_datetime("2015-01-01")].index.values

    stations["month"] = stations["ts"].dt.month
    stations["year"] = stations["ts"].dt.year

    # first convert ws to m/s
    stations["ws"] = stations["ws"].astype(np.float32) / 2.237
    # adjust for height using the log-law: https://websites.pmc.ucsc.edu/~jnoble/wind/extrap/
    # v ~ v_ref * log(z / z_0) / log(z_ref / z_0) where
    # z_0 = 0.5 (roughness length of 0.0005 for "airport" landscape type),
    # z_ref = 10, known speed height (10m)
    # z = 100, approx height of typical wind turbine
    # v_ref is the known speed at z_ref height
    z = 100
    z_ref = 10
    z_0 = 0.5
    stations["ws"] = stations["ws"] * (np.log(z / z_0) / np.log(z_ref / z_0))
    # compute wind energy potential using this:https://byjus.com/wind-energy-formula/
    # rho is air density constant
    rho = 1.23
    stations["wep"] = 0.5 * rho * stations["ws"] ** 3
    wep = stations.drop(columns=["ws", "wd"])
    mean_wep = wep.groupby(["sid", "year", "month"]).mean().reset_index()
    mean_wep["wep"] = np.round(mean_wep["wep"])
    mean_wep = mean_wep.astype({"year": "int16", "month": "int16"})
    outlier_thresholds = (
        mean_wep.groupby(["sid", "month"])["wep"]
        .std()
        .reset_index()
        .rename(columns={"wep": "std"})
    )
    outlier_thresholds["std"] = outlier_thresholds["std"] * 5
    mean_wep = mean_wep.merge(outlier_thresholds, on=["sid", "month"])
    mean_wep = mean_wep[mean_wep["wep"] < mean_wep["std"]].drop(columns="std")

    mean_wep.to_pickle(mean_wep_fp)

    print(f"done, {round(time.perf_counter() - tic, 2)}s")
    print(f"Wind energy potential box plot data saved to {mean_wep_fp}")

    return mean_wep


def main():
    """Execute the preprocessing code"""
    parser = argparse.ArgumentParser(
        description="Pre-process station data for app ingest"
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
    # use the ws_adj column instead of ws
    stations = stations.drop(columns="ws").rename(columns={"ws_adj": "ws"})

    roses_fp = "data/roses.pickle"
    if do_roses:
        discard_obs_fp = base_dir.joinpath("discarded_comparison_obs.csv")
        roses = process_roses(stations, ncpus, roses_fp, discard_obs_fp)

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


if __name__ == "__main__":
    main()    
