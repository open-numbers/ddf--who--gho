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


def load_indicator_list():
    print('reading indicators list from GHO api...')
    inds = 'http://apps.who.int/gho/athena/api/GHO/'
    xml = requests.get(inds)
    indi = xmltodict.parse(xml.content)
    return indi


def process_source_files():
    """create datapoints from source files and return all concepts"""
    indi_list = []
    indi_desc_list = []

    indi = load_indicator_list()

    print('creating datapoint files...')
    # import ipdb; ipdb.set_trace()
    for i in indi['GHO']['Metadata']['Dimension']['Code']:
        path = os.path.join(source_dir, i['@Label']+'.csv')
        concept = to_concept_id(i['@Label'])
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
            except FileNotFoundError:
                print(f'{path} not found')
                continue
            except pd.errors.EmptyDataError:
                print(f'{path} has no data')
                continue
        result, reason = can_proceed(df)
        if result is False:
            print(f'{concept} skipped: {reason}')
            continue
        else:
            create_datapoint(df, concept)
            indi_list.append(concept)
            indi_desc_list.append(i['Display'])

    print('creating concept file...')
    conc = pd.DataFrame([], columns=['concept', 'concept_type', 'name'])
    conc['concept'] = indi_list
    conc['name'] = indi_desc_list
    conc['concept_type'] = 'measure'

    conc = conc.append(pd.DataFrame([['name', 'string', 'Name'],
                                    ['year', 'time', 'Year'],
                                    ['country', 'entity_domain', 'Country']], columns=conc.columns))

    conc_path = os.path.join(out_dir, 'ddf--concepts.csv')
    conc.sort_values(by='concept').to_csv(conc_path, index=False)


def can_proceed(df):
    # TODO: for now I only keep indicators by country/year
    # but later we should also consider other dimensions like
    # age group or sex.
    if df.empty:
        return (False, "empty dataframe")
    if ("COUNTRY" not in df.columns) or ("YEAR" not in df.columns):
        return (False, "no country/year column")
    if df['Numeric'].isnull().all():
        return (False, "no numeric data")
    if (df.dropna(subset=['COUNTRY', 'YEAR'], how='any')
        .duplicated(subset=['COUNTRY', 'YEAR']).any()):
        return (False, "duplicated data found")
    return (True, "")


def create_datapoint(df, concept):
    df.columns = list(map(to_concept_id, df.columns))
    df = df[['country', 'year', 'numeric']]
    df.columns = ['country', 'year', concept]
    df = df.sort_values(by=['country', 'year']).dropna(how='any')
    df['country'] = df['country'].map(to_concept_id)
    df[concept] = df[concept].map(format_float_digits)

    path = os.path.join(out_dir,
                        'ddf--datapoints--{}--by--country--year.csv'.format(concept))
    try:
        df['year'] = df['year'].map(int)
    except ValueError:
        print(f'{concept}: can not convert the year column to int, '
               'removing rows that are not int.')
        df['year'] = df['year'].map(convert_year)
        df = df.dropna(how='any')
    if not df.empty:
        df.to_csv(path, index=False)


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


def convert_year(s):
    try:
        s_ = int(s)
    except ValueError:
        return None
    return str(s_)


if __name__ == '__main__':
    process_source_files()

    ent = extract_entities('COUNTRY')  # TODO: more dimeisions
    ent.to_csv(os.path.join(out_dir, 'ddf--entities--country.csv'), index=False)

    print('Done.')
