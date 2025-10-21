# -*- coding: utf-8 -*-

"""etl script for gho dataset with configurable indicators and multi-dimensional support."""

import pandas as pd
import requests
import os
import json
import html
from collections import defaultdict
from ddf_utils.str import to_concept_id, format_float_digits


# configuration
source_dir = "../source/"
out_dir = "../../"

# Configuration dictionary for indicators to include
# Format: {indicator_code: {options}}
# Options can include filters, transformations, etc.
INDICATORS_CONFIG = {
    # Example configuration - add your indicators here
    # "Adult_curr_cig_smoking": {},
    # "AIR_10": {},
    # "AIR_17": {},
    # Add more indicators as needed
    "GHED_CHE_pc_US_SHA2011": {},
    "GHED_CHEGDP_SHA2011": {},
    "GHED_GGHE-D_pc_US_SHA2011": {},
    "GHED_GGHE-DCHE_SHA2011": {},
    "GHED_GGHE-DGGE_SHA2011": {},
    "GHED_OOPSCHE_SHA2011": {},
    "GHED_PVT-DCHE_SHA2011": {},
    "NCD_BMI_MEANC": {},
    "MORT_100": {},
    "HIV_0000000026": {},
    "HIV_ARTCOVERAGE": {},
    "MDG_0000000026": {},
    "MDG_0000000029": {},
    "NCD_CHOL_MEANTOTALCHOL_C": {},
    "MORT_200": {},
    "RS_196": {},
    "RS_198": {},
    "SA_0000001452": {},
    "SDGSUICIDE": {},
    "VIOLENCE_HOMICIDENUM": {},
    "VIOLENCE_HOMICIDERATE": {},
}


def load_indicator_list():
    """Load indicator metadata from GHO API."""
    print("reading indicators list from GHO api...")
    inds = "https://ghoapi.azureedge.net/api/Indicator"
    json_data = requests.get(inds).json()["value"]

    # Create a dictionary for easy lookup
    return {x["IndicatorCode"]: x for x in json_data if x["Language"] == "EN"}


def flatten_record(record):
    """
    Flatten a GHO record by converting Dim1Type/Dim1, Dim2Type/Dim2, Dim3Type/Dim3
    into direct key-value pairs.

    Example:
        Input: {"Dim1Type": "SEX", "Dim1": "SEX_FMLE", "Dim2Type": "ENVCAUSE", "Dim2": "ENV_01", ...}
        Output: {"SpatialDim": "USA", "TimeDim": 2019, "SEX": "SEX_FMLE", "ENVCAUSE": "ENV_01", ...}
    """
    flattened = {
        "SpatialDim": record.get("SpatialDim"),
        "SpatialDimType": record.get("SpatialDimType"),
        "TimeDim": record.get("TimeDim"),
        "NumericValue": record.get("NumericValue"),
    }

    # Process each dimension (Dim1, Dim2, Dim3)
    for i in [1, 2, 3]:
        dim_type = record.get(f"Dim{i}Type")
        dim_value = record.get(f"Dim{i}")

        # If dimension type and value exist, add as direct key-value pair
        if dim_type is not None and dim_value is not None:
            flattened[dim_type] = dim_value

    return flattened


def get_dimension_columns(records):
    """
    Analyze flattened records to determine which dimension columns are present.
    Returns a set of dimension column names (excluding base columns).
    """
    base_columns = {"SpatialDim", "SpatialDimType", "TimeDim", "NumericValue"}
    all_columns = set()

    for record in records:
        all_columns.update(record.keys())

    # Return only dimension columns (not base columns)
    return all_columns - base_columns


def group_by_dimensions(flattened_records):
    """
    Group flattened records by which dimensions they have.
    Returns a dict mapping frozenset(dimension_columns) -> list of records.
    """
    groups = defaultdict(list)

    for record in flattened_records:
        # Get dimension columns for this record (excluding base columns)
        base_columns = {"SpatialDim", "SpatialDimType", "TimeDim", "NumericValue"}
        dim_cols = frozenset(k for k in record.keys() if k not in base_columns)

        groups[dim_cols].append(record)

    return groups


def extract_entities(dim_type):
    """Read dimension info from GHO API and create entity domain."""
    concept = to_concept_id(dim_type)
    print(f"  - reading {dim_type} ({concept}) info from GHO api...")

    try:
        c_url = f"https://ghoapi.azureedge.net/api/Dimension/{dim_type}/DimensionValues"
        res = requests.get(c_url)
        res.raise_for_status()
        data = res.json()["value"]

        df = pd.DataFrame.from_records(data)
        df = df[["Code", "Title"]].rename(columns={"Code": concept, "Title": "name"})
        df[concept] = df[concept].map(to_concept_id)

        return df
    except Exception as e:
        print(f"    Warning: Could not fetch entities for {dim_type}: {e}")
        return None


def can_proceed(df):
    """Check if dataframe can be processed."""
    if df.empty:
        return (False, "empty dataframe")
    if ("SpatialDim" not in df.columns) or ("TimeDim" not in df.columns):
        return (False, "no country/year column")
    if "NumericValue" not in df.columns or df["NumericValue"].isnull().all():
        return (False, "no numeric data")

    return (True, "")


def convert_year(s):
    """Convert year to integer, return None if invalid."""
    try:
        s_ = int(s)
    except (ValueError, TypeError):
        return None
    return s_


def create_datapoints(flattened_records, concept, indicator_config):
    """
    Create datapoint files for an indicator from flattened records.
    """
    # Filter to only COUNTRY spatial dimension
    country_records = [r for r in flattened_records if r.get("SpatialDimType") == "COUNTRY"]

    if not country_records:
        print(f"  - No country-level data found for {concept}")
        return

    # Group records by dimension combination
    dimension_groups = group_by_dimensions(country_records)

    print(f"  - Found {len(dimension_groups)} dimension combination(s)")

    for dim_cols, records in dimension_groups.items():
        # Convert to list and sort for consistent ordering
        dim_list = sorted(list(dim_cols))

        # Convert records to dataframe-friendly format
        processed_records = []
        for record in records:
            row = {
                "country": to_concept_id(record["SpatialDim"]),
                "year": record["TimeDim"],
                concept: record["NumericValue"],
            }

            # Add dimension values
            for dim_type in dim_list:
                dim_concept = to_concept_id(dim_type)
                row[dim_concept] = to_concept_id(record[dim_type])

            processed_records.append(row)

        # Create dataframe
        df = pd.DataFrame(processed_records)

        # Get all dimension columns in concept_id format
        dim_concepts = [to_concept_id(d) for d in dim_list]
        dim_concepts = [x for x in dim_concepts if x is not None]

        # Check for duplicates
        check_cols = ["country", "year"] + dim_concepts
        if df.duplicated(subset=check_cols).any():
            print(f"    Warning: duplicated data found for dimensions {dim_list}, skipping")
            continue

        # Sort and clean
        df = df.sort_values(by=check_cols).dropna(subset=check_cols + [concept])

        # Convert year to int
        try:
            df["year"] = df["year"].map(int)
        except (ValueError, TypeError):
            print("    Warning: cannot convert year column to int, attempting to clean")
            df["year"] = df["year"].map(convert_year)
            df = df.dropna(subset=["year"])
            df["year"] = df["year"].astype(int)

        # Format numeric values
        df[concept] = df[concept].map(format_float_digits)

        # Reorder columns: country, year, sorted dimensions, then indicator
        column_order = ["country", "year"] + sorted(dim_concepts) + [concept]
        df = df[column_order]

        # Build filename
        all_dims = ["country", "year"] + dim_concepts
        filename = f"ddf--datapoints--{concept}--by--{'--'.join(all_dims)}.csv"
        path = os.path.join(out_dir, filename)

        if not df.empty:
            df.to_csv(path, index=False)
            print(f"    Created: {filename} ({len(df)} records)")


def process_source_files():
    """Create datapoints from source files and return all concepts and dimensions."""
    indicator_metadata = load_indicator_list()

    indi_list = []
    indi_desc_list = []
    all_dimensions = set()

    # If INDICATORS_CONFIG is empty, process all available files
    if not INDICATORS_CONFIG:
        print("Warning: INDICATORS_CONFIG is empty. Add indicators to process.")
        print("Example:")
        print("INDICATORS_CONFIG = {")
        print('    "Adult_curr_cig_smoking": {},')
        print('    "AIR_10": {},')
        print("}")
        return indi_list, indi_desc_list, all_dimensions

    print("creating datapoint files...")

    for indicator_code, config in INDICATORS_CONFIG.items():
        concept_id = to_concept_id(indicator_code)
        path = os.path.join(source_dir, indicator_code + ".json")

        print(f"\nProcessing: {indicator_code} -> {concept_id}")

        if not os.path.exists(path):
            print(f"  - Source file not found: {path}")
            continue

        try:
            with open(path) as f:
                data = json.load(f)["value"]
        except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
            print(f"  - Error loading file: {e}")
            continue
        except Exception as e:
            print(f"  - Unexpected error: {e}")
            continue

        # Convert to dataframe for validation
        df = pd.DataFrame.from_records(data)

        result, reason = can_proceed(df)
        if result is False:
            print(f"  - Skipped: {reason}")
            continue

        # Flatten all records
        flattened_records = [flatten_record(record) for record in data]

        # Collect all dimensions used in this indicator
        dimensions = get_dimension_columns(flattened_records)
        all_dimensions.update(dimensions)

        if dimensions:
            print(f"  - Dimensions found: {', '.join(sorted(dimensions))}")

        # Create datapoints
        create_datapoints(flattened_records, concept_id, config)

        # Add to concept list
        indi_list.append(concept_id)
        if indicator_code in indicator_metadata:
            # Decode HTML entities in indicator name (e.g., &#xb2; -> ²)
            indicator_name = indicator_metadata[indicator_code]["IndicatorName"]
            indi_desc_list.append(html.unescape(indicator_name))
        else:
            indi_desc_list.append(indicator_code)

    return indi_list, indi_desc_list, all_dimensions


def create_concepts(indi_list, indi_desc_list, all_dimensions):
    """Create the concepts file."""
    print("\ncreating concept file...")

    conc = pd.DataFrame({"concept": indi_list, "name": indi_desc_list, "concept_type": "measure"})

    # Add base concepts
    base_concepts = [
        ["name", "string", "Name"],
        ["year", "time", "Year"],
        ["country", "entity_domain", "Country"],
    ]

    # Add dimension concepts
    for dim_type in sorted(all_dimensions):
        dim_concept = to_concept_id(dim_type)
        base_concepts.append([dim_concept, "entity_domain", dim_type.replace("_", " ").title()])

    base_concepts_df = pd.DataFrame(base_concepts, columns=["concept", "concept_type", "name"])

    conc = pd.concat(
        [conc, base_concepts_df],
        ignore_index=True,
    )

    conc_path = os.path.join(out_dir, "ddf--concepts.csv")
    conc.sort_values(by="concept").to_csv(conc_path, index=False)
    print("  - Created: ddf--concepts.csv")


def create_entities(all_dimensions):
    """Create entity files for all dimensions."""
    print("\ncreating entity files...")

    # Always create country entities
    print("Processing dimension: COUNTRY")
    ent = extract_entities("COUNTRY")
    if ent is not None:
        ent_path = os.path.join(out_dir, "ddf--entities--country.csv")
        ent.to_csv(ent_path, index=False)
        print(f"  - Created: ddf--entities--country.csv ({len(ent)} entities)")

    # Create entities for other dimensions
    for dim_type in sorted(all_dimensions):
        print(f"Processing dimension: {dim_type}")
        ent = extract_entities(dim_type)
        if ent is not None:
            dim_concept = to_concept_id(dim_type)
            ent_path = os.path.join(out_dir, f"ddf--entities--{dim_concept}.csv")
            ent.to_csv(ent_path, index=False)
            print(f"  - Created: ddf--entities--{dim_concept}.csv ({len(ent)} entities)")


if __name__ == "__main__":
    print("=" * 60)
    print("WHO GHO DDF ETL Script")
    print("=" * 60)

    # Process source files
    indi_list, indi_desc_list, all_dimensions = process_source_files()

    if indi_list:
        # Create concepts file
        create_concepts(indi_list, indi_desc_list, all_dimensions)

        # Create entity files
        create_entities(all_dimensions)

        print("\n" + "=" * 60)
        print(
            f"Done! Processed {len(indi_list)} indicators with {len(all_dimensions)} additional dimensions."
        )
        print("=" * 60)
    else:
        print("\nNo indicators processed. Please check INDICATORS_CONFIG.")
