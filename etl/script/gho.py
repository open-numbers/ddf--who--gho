# -*- coding: utf-8 -*-

"""etl script for gho dataset."""

import pandas as pd
import xmltodict
import requests
import os
import numpy as np
from ddf_utils.str import to_concept_id
from ddf_utils.index import create_index_file


# configuration
source_dir = '../source/'
out_dir = '../../'


def extract_concepts():
    print('reading indicators list from GHO api...')
    inds = 'http://apps.who.int/gho/athena/api/GHO/'
    xml = requests.get(inds)
    indi = xmltodict.parse(xml.content)

    indi_list = []
    indi_desc_list = []

    print('creating concept files...')
    for i in indi['GHO']['Metadata']['Dimension']['Code']:
        path = os.path.join(source_dir, i['@Label']+'.csv')
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
            except:
                print('failed to read: '+path)
                continue

            if (len(df) > 0
                    and 'COUNTRY' in df.columns
                    and 'YEAR' in df.columns):
                indi_list.append(i['@Label'])
                indi_desc_list.append(i['Display'])

    conc = pd.DataFrame([], columns=['concept', 'concept_type', 'name'])
    conc['concept'] = indi_list
    conc['name'] = indi_desc_list
    conc['concept_type'] = 'measure'

    conc = conc.append(pd.DataFrame([['name', 'string', 'Name'],
                                    ['year', 'time', 'Year'],
                                    ['country', 'entity_domain', 'Country']], columns=conc.columns))

    conc['concept'] = conc['concept'].map(to_concept_id)

    return conc


def extract_entities():
    print('reading country info from GHO api...')
    c_url = 'http://apps.who.int/gho/athena/api/COUNTRY'
    cr = requests.get(c_url)
    cp = xmltodict.parse(cr.content)

    c = {}

    print('creating entities files...')
    for i in cp['GHO']['Metadata']['Dimension']['Code']:
        attrs = i['Attr']
        c[i['@Label']] = dict([(attr['@Category'], attr['Value']['Display']) for attr in attrs])

    cdf = pd.DataFrame.from_dict(c, orient='index')
    cdf.index.name = 'country'

    return cdf.reset_index()


def extract_datapoints(conc):
    """conc: a list of all concepts"""
    print('creating datapoints files...')

    res = {}

    for f in os.listdir(source_dir):
        if '.csv' in f:
            concept = to_concept_id(f[:-4])

            if concept not in conc:
                continue

            df = pd.read_csv(os.path.join(source_dir, f))
            df.columns = list(map(to_concept_id, df.columns))

            if np.all(df['numeric'].isnull()) == True:
                df = df[['country', 'year', 'display_value']]
            else:
                df = df[['country', 'year', 'numeric']]

            df.columns = [['country', 'year', concept]]

            res[concept] = df.sort_values(by=['country', 'year'])\
                             .dropna(how='any')

    return res


if __name__ == '__main__':
    conc = extract_concepts()
    ent = extract_entities()
    dps = extract_datapoints(conc['concept'].values)

    conc.to_csv(os.path.join(out_dir, 'ddf--concepts.csv'), index=False)
    ent.to_csv(os.path.join(out_dir, 'ddf--entities--country.csv'), index=False)

    for k, df in dps.items():
        path = os.path.join(out_dir,
                            'ddf--datapoints--{}--by--country--year.csv'.format(k))
        df.to_csv(path, index=False)

    create_index_file(out_dir)

    print('Done.')


