# -*- coding: utf-8 -*-

"""etl script for gho dataset."""

import pandas as pd
import requests
import os
import json
from ddf_utils.str import to_concept_id, format_float_digits


# configuration
source_dir = "../source/"
out_dir = "../../"


def load_indicator_list():
    print("reading indicators list from GHO api...")
    inds = "https://ghoapi.azureedge.net/api/Indicator"
    json_data = requests.get(inds).json()["value"]

    return [x for x in json_data if x["Language"] == "EN"]


def process_source_files():
    """create datapoints from source files and return all concepts"""
    indi_list = []
    indi_desc_list = []

    indi = load_indicator_list()

    print("creating datapoint files...")
    # import ipdb; ipdb.set_trace()
    for i in indi:
        concept_id = i["IndicatorCode"]
        path = os.path.join(source_dir, concept_id + ".json")
        concept = to_concept_id(concept_id)
        if os.path.exists(path):
            try:
                with open(path) as f:
                    data = json.load(f)["value"]
                df = pd.DataFrame.from_records(data)
            except (FileNotFoundError, KeyError, json.JSONDecodeError):
                print(f"{path} not found or is invalid")
                continue
            except pd.errors.EmptyDataError:
                print(f"{path} has no data")
                continue
        else:
            continue

        if not df.empty and (
            df["Dim1"].notnull().any() or df["Dim2"].notnull().any() or df["Dim3"].notnull().any()
        ):
            print(f"indicator {concept} has other dimensions, skipping.")
            continue

        result, reason = can_proceed(df)
        if result is False:
            print(f"{concept} skipped: {reason}")
            continue
        else:
            create_datapoint(df, concept)
            indi_list.append(concept)
            indi_desc_list.append(i["IndicatorName"])

    print("creating concept file...")
    conc = pd.DataFrame([], columns=["concept", "concept_type", "name"])
    conc["concept"] = indi_list
    conc["name"] = indi_desc_list
    conc["concept_type"] = "measure"

    conc = pd.concat(
        [
            conc,
            pd.DataFrame(
                [
                    ["name", "string", "Name"],
                    ["year", "time", "Year"],
                    ["country", "entity_domain", "Country"],
                ],
                columns=conc.columns,
            ),
        ],
        ignore_index=True,
    )

    conc_path = os.path.join(out_dir, "ddf--concepts.csv")
    conc.sort_values(by="concept").to_csv(conc_path, index=False)


def can_proceed(df):
    # TODO: for now I only keep indicators by country/year
    # but later we should also consider other dimensions like
    # age group or sex.
    if df.empty:
        return (False, "empty dataframe")
    if ("SpatialDim" not in df.columns) or ("TimeDim" not in df.columns):
        return (False, "no country/year column")
    if "NumericValue" not in df.columns or df["NumericValue"].isnull().all():
        return (False, "no numeric data")
    if (
        df.dropna(subset=["SpatialDim", "TimeDim"], how="any")
        .duplicated(subset=["SpatialDim", "TimeDim"])
        .any()
    ):
        return (False, "duplicated data found")
    return (True, "")


def create_datapoint(df, concept):
    df = df[df["SpatialDimType"] == "COUNTRY"].copy()
    df = df[["SpatialDim", "TimeDim", "NumericValue"]]
    df.columns = ["country", "year", concept]
    df = df.sort_values(by=["country", "year"]).dropna(how="any")
    df["country"] = df["country"].map(to_concept_id)
    df[concept] = df[concept].map(format_float_digits)

    path = os.path.join(out_dir, "ddf--datapoints--{}--by--country--year.csv".format(concept))
    try:
        df["year"] = df["year"].map(int)
    except ValueError:
        print(f"{concept}: can not convert the year column to int, removing rows that are not int.")
        df["year"] = df["year"].map(convert_year)
        df = df.dropna(how="any")
    if not df.empty:
        df.to_csv(path, index=False)


def extract_entities(dim):
    """read dimension info from GHO and create entity domain"""
    concept = dim.lower()
    print(f"reading {concept} info from GHO api...")
    c_url = f"https://ghoapi.azureedge.net/api/Dimension/{dim}/DimensionValues"
    res = requests.get(c_url)
    data = res.json()["value"]

    df = pd.DataFrame.from_records(data)

    print("creating entities files...")

    df = df[["Code", "Title"]].rename(columns={"Code": concept, "Title": "name"})
    df[concept] = df[concept].map(to_concept_id)
    return df


def convert_year(s):
    try:
        s_ = int(s)
    except ValueError:
        return None
    return str(s_)


if __name__ == "__main__":
    process_source_files()

    ent = extract_entities("COUNTRY")  # TODO: more dimeisions
    ent.to_csv(os.path.join(out_dir, "ddf--entities--country.csv"), index=False)

    print("Done.")
