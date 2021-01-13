"""Download hisotorical wind data from SNAP CKAN"""

import subprocess
from pathlib import Path


if __name__ == '__main__':
    # get URIs from CKAN dir
    ckan_dir = "http://data.snap.uaf.edu/data/Base/AK_WRF/Historical_Projected_Hourly_Winds_1980_2099/asos"
    out = subprocess.check_output(["wget", "-qO-", ckan_dir])
    uris = ["http://data.snap.uaf.edu/" + x.split("\\'>")[0] for x in str(out).split("href=\\\'") if ".csv" in x]
    # download to data dir
    asos_dir = Path.cwd().joinpath("data/asos")
    asos_dir.mkdir(parents=True, exist_ok=True)
    _ = [subprocess.Popen(["wget", uri, "-q", "-P", asos_dir]) for uri in uris]
