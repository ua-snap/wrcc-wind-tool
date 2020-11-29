"""Download hisotorical wind data from SNAP CKAN"""

import os, subprocess


if __name__ == '__main__':
    # get URIs from CKAN dir
    ckan_dir = "http://data.snap.uaf.edu/data/Base/AK_WRF/Historical_Projected_Hourly_Winds_1980_2099/asos"
    out = subprocess.check_output(["wget", "-qO-", ckan_dir])
    uris = ["http://data.snap.uaf.edu/" + x.split("\\'>")[0] for x in str(out).split("href=\\\'") if ".csv" in x]
    # download to data dir
    _ = [subprocess.Popen(["wget", uri, "-q", "-P", "data"]) for uri in uris]
