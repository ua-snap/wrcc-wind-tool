"""Process raw IEM data, output single optimized pickle file"""

# USE THIS SCRIPT TO DO QC DATA PROCESSING
# hacky, done to alllow import from luts.py in app dir
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse, glob, time
import numpy as np
import pandas as pd
import ruptures as rpt
from bias_correction import BiasCorrection
from luts import decades
from multiprocessing import Pool
from pathlib import Path
from scipy.signal import find_peaks


def filter_spurious(station, xname="sped", tname="ts", delta=30):
    """Identify and remove spurious observations, 
    returns filtered data and flagged observations"""
    station = station.set_index(tname)
    # ignore missing speed data
    ws_series = station[~np.isnan(station["sped"])]["sped"]
    # identify and remove completely obvious peaks, to help with dip detection
    obv_peaks, _ = find_peaks(ws_series, prominence=30, threshold=50)
    # if-else in case no obvious spikes
    if obv_peaks.shape[0] != 0:
        obv_spikes = ws_series[obv_peaks]
        ws_series = ws_series.drop(obv_spikes.index)
    else:
        obv_spikes = pd.Series()

    # invert series, identify dips using less strict criteria
    dip_peaks, _ = find_peaks(ws_series * -1, prominence=30, threshold=35)
    # if-else in case no dips found
    if dip_peaks.shape[0] != 0:
        dips = ws_series[dip_peaks]
        ws_series = ws_series.drop(dips.index)
    else:
        dips = pd.Series()

    # identify less obvious peaks
    peaks, properties = find_peaks(ws_series, prominence=25, width=(None, 2))
    # combine with obvious peaks if present
    if peaks.shape[0] != 0:
        # condition on width_heights property to reduce sensitivty (see ancillary/raw_qc.ipynb)
        spikes = pd.concat(
            [obv_spikes, ws_series[peaks[properties["width_heights"] >= 18]]]
        )
    else:
        spikes = pd.concat([obv_spikes, pd.Series()])

    # subset the station data to keep these flagged observations,
    # then remove from station data
    if dips.size != 0:
        dips = station.loc[dips.index]
        station = station.drop(dips.index)
    else:
        # take empty slice
        dips = station[station["station"] == "cats"]

    if spikes.size != 0:
        # subset station data frame for spikes and dips
        spikes = station.loc[spikes.index]
        station = station.drop(spikes.index)
    else:
        # take empty slice
        dips = station[station["station"] == "dogs"]

    return station.reset_index(), spikes.reset_index(), dips.reset_index()


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
        station.groupby("ts")
        .agg(min_delta_dt=pd.NamedAgg("delta_dt", "min"))
        .reset_index()
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


def adjust_station(station):
    """Detects changepoints in wind speed time series and
    performs quantile mapping"""

    def get_changepoints(arr):
        """Helper function to find and return the changepoints in a wind speed
        time series array"""
        algo = rpt.Pelt(jump=1).fit(arr)
        # 260 determined to be the smallest penalty where
        # the most changepoints detected across all stations
        # was 2.
        return algo.predict(pen=260)

    # aggregate by month for changepoint detection
    station["ym"] = station["ts"].dt.year.astype("str") + "_" + station["ts"].dt.month.astype("str")
    monthly_means = station.groupby("ym", sort=False, as_index=False)["sped"].mean()

    # detect changepoints
    cpts = get_changepoints(monthly_means["sped"].values)
    
    # if no changepoints found, set adjusted ws to 
    if len(cpts) == 1:
        station["sped_adj"] = station["sped"]
        changepoints = pd.DataFrame({"sid": [], "cpt_date": []}) # no change points, empty df
        station = station.drop(columns="ym")

        return station, changepoints
    # otherwise, slice up wind speed time series based on 
    # year-months of changepoints
    
    # determine slices
    cpts_ym = [monthly_means.iloc[cp]["ym"] for cp in cpts[:-1]]
    cpt_dates = [pd.to_datetime(f"{ym[:4]}-{ym[5:]}-01") for ym in cpts_ym]
    slices = [slice("1980-01-01", cpt_dates[0])]
    if len(cpts) == 3:
        # if difference between 1st and second breakpoint is 
        # not greater than 5 years, use first breakpoint
        if (cpt_dates[1] - cpt_dates[0]).days / 365 > 5:
            slices.append(slice(cpt_dates[0], cpt_dates[1]))
            slices.append(slice(cpt_dates[-1], "2019-12-31"))
        else:
            cpt_dates = cpt_dates[:1]
            slices.append(slice(cpt_dates[0], "2019-12-31"))
    
    station = station.set_index("ts")
    # "observed", or unbiased, slice taken to be the most recent
    obs = station[slices[-1]]["sped"].values
    # "simulated" or biased data are the more historical slices
    sim = [station[sl]["sped"].values for sl in slices[:-1]]
    
    station["sped_adj"] = station["sped"]
    for sl in slices[:-1]:
        sim = station[sl]["sped"].values
        bc = BiasCorrection(obs, sim, sim)
        station.loc[sl, "sped_adj"] = bc.correct()
        
    # construct changepoints dataframe for logging
    changepoints = pd.DataFrame({"sid": station["station"].iloc[0], "cpt_date": cpt_dates})
    station = station.drop(columns="ym").reset_index()

    return station, changepoints


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

    # Filter out ridiculously fast spikes
    # 100mph may be too low, use 110
    stations = station = station[station["sped"] < 110]

    # handle observations with erroneous combination of direction/speed
    # The case where speed is 0 and direction is not is and invalid measurement.
    # Not common (<600 for entire dataset), and there is no way to know whether the error was
    # in speed or direction measurement. Drop them.
    station = station.drop(
        station[
            (station["sped"] == 0)
            & (~np.isnan(station["drct"]))
            & (station["drct"] != 0)
        ].index
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

    # Filter observations!
    # Filter to hourly obs closest to "on the hour"
    station = filter_to_hour(station)
    # filter out spikes and dips using scipy.signal.find_peaks,
    # keep those obs just in case
    station, spikes, dips = filter_spurious(station)

    # detect changes in wind regime and adjust via quantile mapping
    station, changepoints = adjust_station(station)

    # add decade column for aggregating
    for dyear in decades:
        dyears = list(range(dyear, dyear + 10))
        station.loc[station["ts"].dt.year.isin(dyears), "decade"] = decades[dyear]

    return (station, spikes, dips, changepoints)


def run_read_and_process(fps, ncpus):
    """Read the raw data files

    Args:
        fps (list): list of filepaths of IEM ASOS data

    Returns:
        pandas.DataFrame of all individual sites
    
    Notes:
        The timestamp in the returned data is the NEAREST HOUR of the obs
    """
    print(f"Reading / processing data using {ncpus} cores", end="...")
    tic = time.perf_counter()

    with Pool(ncpus) as pool:
        out = pool.map(read_and_process_csv, fps)

    stations = pd.concat([out_tuple[0] for out_tuple in out], ignore_index=True)
    spikes = pd.concat([out_tuple[1] for out_tuple in out], ignore_index=True)
    dips = pd.concat([out_tuple[2] for out_tuple in out], ignore_index=True)
    changepoints = pd.concat([out_tuple[3] for out_tuple in out], ignore_index=True)

    print(f"done, {round(time.perf_counter() - tic, 2)}s")

    return (
        stations.rename(columns={"station": "sid", "sped": "ws", "sped_adj": "ws_adj", "drct": "wd"}),
        spikes,
        dips,
        changepoints,
    )


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

    # ignore stations identified as insufficient in raw_qc.ipynb
    discard = pd.read_csv(base_dir.joinpath("discard.csv"))
    raw_fps = [
        fp for fp in raw_fps if str(fp).split("_")[-2] not in discard["sid"].values
    ]

    out_fp = base_dir.joinpath("stations.pickle")
    spikes_fp = base_dir.joinpath("raw_spikes.pickle")
    dips_fp = base_dir.joinpath("raw_dips.pickle")
    cpts_fp = base_dir.joinpath("raw_changepoints.pickle")

    # read data
    stations, spikes, dips, changepoints = run_read_and_process(raw_fps, ncpus)

    print(f"Writing processed data to {out_fp}")
    print(f"Writing filtered spikes data to {spikes_fp}")
    print(f"Writing filtered dips data to {dips_fp}")
    print(f"Writing changepoints data to {cpts_fp}")

    tic = time.perf_counter()
    stations.to_pickle(out_fp)
    spikes.to_pickle(spikes_fp)
    dips.to_pickle(dips_fp)
    changepoints.to_pickle(cpts_fp)
    print(f"Done, {round(time.perf_counter() - tic, 2)}s")

    return None


if __name__ == "__main__":
    main()
