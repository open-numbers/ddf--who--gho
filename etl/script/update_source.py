# -*- coding: utf-8 -*-

"""download all source files from gho API."""

import xmltodict
import requests
import os
import pandas as pd
import asyncio
import os, signal
from concurrent.futures import ProcessPoolExecutor


pool_size = 2


def download(i):
    url = 'http://apps.who.int/gho/athena/data/data-coded.csv?target=GHO/{}'.format(i)
    res = requests.get(url)
    if res.status_code != 200:
        print("failed to download: {}, code: {}".format(i, res.status_code))
        return (False, i)
    with open('../source/{}.csv'.format(i), 'wb') as f:
        f.write(res.content)
    return (True, i)


def run_download(todos):
    with ProcessPoolExecutor(max_workers=pool_size) as executor:
        result = executor.map(download, todos)
    return result


def main():
    inds = 'http://apps.who.int/gho/athena/api/GHO/'
    xml = requests.get(inds)
    xml = xml.content

    indi = xmltodict.parse(xml)

    indi_list = []

    for i in indi['GHO']['Metadata']['Dimension']['Code']:
        # print(i)
        indi_list.append(i['@Label'])

    print('{} files to be downloaded.'.format(len(indi_list)) )

    result = run_download(indi_list)
    for status, i in result:
        if status is False:
            print(i)


if __name__ == '__main__':
    print('Downloading source files...')
    main()
