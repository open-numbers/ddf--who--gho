# -*- coding: utf-8 -*-

"""etl script for gho dataset."""

import pandas as pd
import xmltodict
import requests
import os
import numpy as np
from ddf_utils.str import to_concept_id, format_float_digits


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
    # import ipdb; ipdb.set_trace()
    for i in indi['GHO']['Metadata']['Dimension']['Code']:
        path = os.path.join(source_dir, i['@Label']+'.csv')
        concept = to_concept_id(i['@Label'])
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
            except FileNotFoundError:
                print('not found: '+path)
                continue
            except pd.errors.EmptyDataError:
                print('no data: '+path)
                continue

            # TODO: for now I only keep indicators by country/year
            # but later we should also consider other dimensions like
            # age group or sex.
            if (not df.empty
                and (set(df.columns) ==
                     set(["GHO","PUBLISHSTATE","YEAR","REGION","COUNTRY","Display Value",
                          "Numeric","Low","High","Comments"]))):
                indi_list.append(concept)
                indi_desc_list.append(i['Display'])
            else:
                print('{}.csv: empty data or primary keys not "country,year"'.format(i['@Label']))

    conc = pd.DataFrame([], columns=['concept', 'concept_type', 'name'])
    conc['concept'] = indi_list
    conc['name'] = indi_desc_list
    conc['concept_type'] = 'measure'

    conc = conc.append(pd.DataFrame([['name', 'string', 'Name'],
                                    ['year', 'time', 'Year'],
                                    ['country', 'entity_domain', 'Country']], columns=conc.columns))

    return conc


def extract_entities(dim):
    """read dimension info from GHO and create entity domain"""
    concept = dim.lower()
    print(f'reading {concept} info from GHO api...')
    c_url = f'http://apps.who.int/gho/athena/api/{dim}'
    cr = requests.get(c_url)
    cp = xmltodict.parse(cr.content)

    c = {}

    print('creating entities files...')
    for i in cp['GHO']['Metadata']['Dimension']['Code']:
        attrs = i['Attr']
        c[i['@Label']] = dict([(attr['@Category'], attr['Value']['Display']) for attr in attrs])

    cdf = pd.DataFrame.from_dict(c, orient='index')
    cdf.index.name = concept

    cdf = cdf.reset_index()
    cdf = cdf[[concept, 'SHORTNAMEES']].rename(columns={'SHORTNAMEES': 'name'})
    cdf[concept] = cdf[concept].map(to_concept_id)
    return cdf


def extract_datapoints(conc):
    """conc: a list of all concepts"""
    print('creating datapoints files...')

    res = {}

    for f in os.listdir(source_dir):
        if '.csv' in f:
            concept = to_concept_id(f[:-4])

            if concept not in conc:
                # print(f'{concept} was not found in the GHO indicator list. Skipping it.')
                continue

            df = pd.read_csv(os.path.join(source_dir, f))

            # skip empty files
            if df.empty:
                # print(f'no data for {concept}')
                continue

            if (set(df.columns) !=
                set(["GHO","PUBLISHSTATE","YEAR","REGION","COUNTRY","Display Value",
                     "Numeric","Low","High","Comments"])):
                # print(f'{concept}: dimensions for this indicator are not "country, year", skipping it.')
                continue

            df.columns = list(map(to_concept_id, df.columns))

            if np.all(df['numeric'].isnull()) == True:
                # df = df[['country', 'year', 'display_value']]
                print(f'skipping {concept} because no numeric values')
                continue
            else:
                df = df[['country', 'year', 'numeric']]

            df.columns = ['country', 'year', concept]

            df = df.sort_values(by=['country', 'year']).dropna(how='any')
            df['country'] = df['country'].map(to_concept_id)
            df[concept] = df[concept].map(format_float_digits)
            res[concept] = df

    return res


def convert_year(s):
    try:
        s_ = int(s)
    except ValueError:
        return None
    return str(s_)


if __name__ == '__main__':
    conc = extract_concepts()
    ent = extract_entities('COUNTRY')  # TODO: more dimeisions
    dps = extract_datapoints(conc['concept'].values)

    conc.to_csv(os.path.join(out_dir, 'ddf--concepts.csv'), index=False)
    ent.to_csv(os.path.join(out_dir, 'ddf--entities--country.csv'), index=False)

    for k, df in dps.items():
        path = os.path.join(out_dir,
                            'ddf--datapoints--{}--by--country--year.csv'.format(k))
        try:
            df['year'] = df['year'].map(int)
        except ValueError:
            print(f'{k}: can not convert the year column to int, removing rows that are not int.')
            df['year'] = df['year'].map(convert_year)
            df = df.dropna(how='any')
        if not df.empty:
            df.to_csv(path, index=False)

    print('Done.')
