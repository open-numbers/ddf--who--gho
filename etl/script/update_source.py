# -*- coding: utf-8 -*-

"""download all source files from gho API."""

import requests
from concurrent.futures import ProcessPoolExecutor
import json


pool_size = 5


def download(i):
    url = f"https://ghoapi.azureedge.net/api/{i}"
    res = requests.get(url)
    if res.status_code != 200:
        print("failed to download: {}, code: {}".format(i, res.status_code))
        return (False, i)
    with open("../source/{}.json".format(i), "w") as f:
        json.dump(res.json(), f)
    return (True, i)


def run_download(todos):
    with ProcessPoolExecutor(max_workers=pool_size) as executor:
        result = executor.map(download, todos)
    return result


def main():
    inds = "https://ghoapi.azureedge.net/api/Indicator"
    json_data = requests.get(inds).json()["value"]

    # IndicatorCode - use for filename and concept id
    # IndicatorName - use for concept name
    # Language - only use English data
    indi_list = [x["IndicatorCode"] for x in json_data if x["Language"] == "EN"]

    print("{} files to be downloaded.".format(len(indi_list)))

    result = run_download(indi_list)
    for status, i in result:
        if status is False:
            print(i)


if __name__ == "__main__":
    print("Downloading source files...")
    main()
