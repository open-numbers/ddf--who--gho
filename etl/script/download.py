# -*- coding: utf-8 -*-

"""download all source files from gho API."""

import xmltodict
import requests
import os
import pandas as pd


api = 'http://apps.who.int/gho/athena/api/'


def download():
    inds = 'http://apps.who.int/gho/athena/api/GHO/'
    xml = requests.get(inds)
    xml = xml.content

    indi = xmltodict.parse(xml)
    indi_list = []

    for i in indi['GHO']['Metadata']['Dimension']['Code']:
    #     print(i)
        indi_list.append(i['@Label'])

    for i in indi_list:
        if os.path.exists('source/{}.csv'.format(i)):
            continue

        url = 'http://apps.who.int/gho/athena/api/GHO/{}.csv'.format(i)

        # TODO
