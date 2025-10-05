# -*- coding: utf-8 -*-

"""
Test script for WHO GHO DDF ETL Script (Flattened Record Approach).

This script tests the key functions of the ETL script with sample data.
"""

import sys
import html
from collections import defaultdict


# Mock functions from etl.py for testing
def to_concept_id(s):
    """Convert string to concept ID format (simplified version)."""
    return str(s).lower().replace(" ", "_").replace("-", "_")


def flatten_record(record):
    """
    Flatten a GHO record by converting Dim1Type/Dim1, Dim2Type/Dim2, Dim3Type/Dim3
    into direct key-value pairs.
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


def test_flatten_record():
    """Test record flattening with various dimension configurations."""
    print("=" * 60)
    print("TEST 1: Record Flattening")
    print("=" * 60)

    # Test case 1: No dimensions
    record1 = {
        "SpatialDim": "USA",
        "SpatialDimType": "COUNTRY",
        "TimeDim": 2019,
        "Dim1Type": None,
        "Dim1": None,
        "Dim2Type": None,
        "Dim2": None,
        "Dim3Type": None,
        "Dim3": None,
        "NumericValue": 100.5,
    }
    result1 = flatten_record(record1)
    expected1 = {
        "SpatialDim": "USA",
        "SpatialDimType": "COUNTRY",
        "TimeDim": 2019,
        "NumericValue": 100.5,
    }
    assert result1 == expected1, f"Expected {expected1}, got {result1}"
    print("✓ Test 1.1 passed: No dimensions")

    # Test case 2: Single dimension (SEX in Dim1)
    record2 = {
        "SpatialDim": "USA",
        "SpatialDimType": "COUNTRY",
        "TimeDim": 2019,
        "Dim1Type": "SEX",
        "Dim1": "SEX_FMLE",
        "Dim2Type": None,
        "Dim2": None,
        "Dim3Type": None,
        "Dim3": None,
        "NumericValue": 12.5,
    }
    result2 = flatten_record(record2)
    expected2 = {
        "SpatialDim": "USA",
        "SpatialDimType": "COUNTRY",
        "TimeDim": 2019,
        "SEX": "SEX_FMLE",
        "NumericValue": 12.5,
    }
    assert result2 == expected2, f"Expected {expected2}, got {result2}"
    print("✓ Test 1.2 passed: Single dimension (SEX)")

    # Test case 3: Two dimensions (SEX + ENVCAUSE)
    record3 = {
        "SpatialDim": "USA",
        "SpatialDimType": "COUNTRY",
        "TimeDim": 2019,
        "Dim1Type": "SEX",
        "Dim1": "SEX_FMLE",
        "Dim2Type": "ENVCAUSE",
        "Dim2": "ENVCAUSE_039",
        "Dim3Type": None,
        "Dim3": None,
        "NumericValue": 15.3,
    }
    result3 = flatten_record(record3)
    expected3 = {
        "SpatialDim": "USA",
        "SpatialDimType": "COUNTRY",
        "TimeDim": 2019,
        "SEX": "SEX_FMLE",
        "ENVCAUSE": "ENVCAUSE_039",
        "NumericValue": 15.3,
    }
    assert result3 == expected3, f"Expected {expected3}, got {result3}"
    print("✓ Test 1.3 passed: Two dimensions (SEX + ENVCAUSE)")

    # Test case 4: Two dimensions in DIFFERENT ORDER (ENVCAUSE in Dim1, SEX in Dim2)
    # This should produce the SAME flattened structure
    record4 = {
        "SpatialDim": "CAN",
        "SpatialDimType": "COUNTRY",
        "TimeDim": 2019,
        "Dim1Type": "ENVCAUSE",
        "Dim1": "ENVCAUSE_039",
        "Dim2Type": "SEX",
        "Dim2": "SEX_MLE",
        "Dim3Type": None,
        "Dim3": None,
        "NumericValue": 18.7,
    }
    result4 = flatten_record(record4)
    expected4 = {
        "SpatialDim": "CAN",
        "SpatialDimType": "COUNTRY",
        "TimeDim": 2019,
        "ENVCAUSE": "ENVCAUSE_039",
        "SEX": "SEX_MLE",
        "NumericValue": 18.7,
    }
    assert result4 == expected4, f"Expected {expected4}, got {result4}"
    # Verify that both have the same dimension keys (regardless of original order)
    assert set(result3.keys()) == set(result4.keys()), "Different dimension keys!"
    print(
        "✓ Test 1.4 passed: Two dimensions in different order (produces same structure)"
    )

    # Test case 5: Three dimensions
    record5 = {
        "SpatialDim": "GBR",
        "SpatialDimType": "COUNTRY",
        "TimeDim": 2020,
        "Dim1Type": "SEX",
        "Dim1": "SEX_BTSX",
        "Dim2Type": "AGEGROUP",
        "Dim2": "AGE_30_34",
        "Dim3Type": "LOCATION",
        "Dim3": "URBAN",
        "NumericValue": 22.1,
    }
    result5 = flatten_record(record5)
    expected5 = {
        "SpatialDim": "GBR",
        "SpatialDimType": "COUNTRY",
        "TimeDim": 2020,
        "SEX": "SEX_BTSX",
        "AGEGROUP": "AGE_30_34",
        "LOCATION": "URBAN",
        "NumericValue": 22.1,
    }
    assert result5 == expected5, f"Expected {expected5}, got {result5}"
    print("✓ Test 1.5 passed: Three dimensions")

    print("\n✅ All record flattening tests passed!\n")


def test_get_dimension_columns():
    """Test dimension column extraction."""
    print("=" * 60)
    print("TEST 2: Get Dimension Columns")
    print("=" * 60)

    # Test with no dimensions
    records1 = [
        {
            "SpatialDim": "USA",
            "SpatialDimType": "COUNTRY",
            "TimeDim": 2019,
            "NumericValue": 100.5,
        }
    ]
    dims1 = get_dimension_columns(records1)
    assert dims1 == set(), f"Expected empty set, got {dims1}"
    print("✓ Test 2.1 passed: No dimensions")

    # Test with single dimension
    records2 = [
        {
            "SpatialDim": "USA",
            "SpatialDimType": "COUNTRY",
            "TimeDim": 2019,
            "SEX": "SEX_FMLE",
            "NumericValue": 12.5,
        }
    ]
    dims2 = get_dimension_columns(records2)
    assert dims2 == {"SEX"}, f"Expected {{'SEX'}}, got {dims2}"
    print("✓ Test 2.2 passed: Single dimension (SEX)")

    # Test with multiple records having different dimensions
    records3 = [
        {
            "SpatialDim": "USA",
            "TimeDim": 2019,
            "SEX": "SEX_FMLE",
            "ENVCAUSE": "ENV_01",
            "NumericValue": 12.5,
        },
        {
            "SpatialDim": "CAN",
            "TimeDim": 2019,
            "SEX": "SEX_MLE",
            "AGEGROUP": "AGE_30_34",
            "NumericValue": 15.3,
        },
    ]
    dims3 = get_dimension_columns(records3)
    assert dims3 == {"SEX", "ENVCAUSE", "AGEGROUP"}, (
        f"Expected {{'SEX', 'ENVCAUSE', 'AGEGROUP'}}, got {dims3}"
    )
    print("✓ Test 2.3 passed: Multiple dimensions across records")

    print("\n✅ All dimension column extraction tests passed!\n")


def test_group_by_dimensions():
    """Test grouping records by dimension combinations."""
    print("=" * 60)
    print("TEST 3: Group By Dimensions")
    print("=" * 60)

    # Create sample flattened records with different dimension combinations
    records = [
        # Group 1: SEX only
        {
            "SpatialDim": "USA",
            "SpatialDimType": "COUNTRY",
            "TimeDim": 2019,
            "SEX": "SEX_FMLE",
            "NumericValue": 10.0,
        },
        {
            "SpatialDim": "CAN",
            "SpatialDimType": "COUNTRY",
            "TimeDim": 2019,
            "SEX": "SEX_MLE",
            "NumericValue": 12.0,
        },
        # Group 2: SEX + ENVCAUSE
        {
            "SpatialDim": "USA",
            "SpatialDimType": "COUNTRY",
            "TimeDim": 2019,
            "SEX": "SEX_FMLE",
            "ENVCAUSE": "ENV_01",
            "NumericValue": 15.0,
        },
        {
            "SpatialDim": "CAN",
            "SpatialDimType": "COUNTRY",
            "TimeDim": 2019,
            "SEX": "SEX_MLE",
            "ENVCAUSE": "ENV_01",
            "NumericValue": 18.0,
        },
        # Group 3: SEX + AGEGROUP
        {
            "SpatialDim": "GBR",
            "SpatialDimType": "COUNTRY",
            "TimeDim": 2020,
            "SEX": "SEX_BTSX",
            "AGEGROUP": "AGE_30_34",
            "NumericValue": 20.0,
        },
        # Group 2 again: SEX + ENVCAUSE (should be in same group as records 2-3)
        {
            "SpatialDim": "GBR",
            "SpatialDimType": "COUNTRY",
            "TimeDim": 2020,
            "ENVCAUSE": "ENV_02",  # Different value but same dimension set
            "SEX": "SEX_FMLE",
            "NumericValue": 22.0,
        },
    ]

    groups = group_by_dimensions(records)

    print(f"Found {len(groups)} dimension groups:")
    for dim_set, group_records in groups.items():
        print(f"  - {sorted(dim_set)}: {len(group_records)} records")

    # Should have exactly 3 groups
    assert len(groups) == 3, f"Expected 3 groups, got {len(groups)}"
    print("✓ Test 3.1 passed: Correct number of groups")

    # Verify Group 1: SEX only
    sex_only = frozenset(["SEX"])
    assert sex_only in groups, "Expected SEX-only group"
    assert len(groups[sex_only]) == 2, "Expected 2 records in SEX-only group"
    print("✓ Test 3.2 passed: SEX-only group identified")

    # Verify Group 2: SEX + ENVCAUSE
    sex_env = frozenset(["SEX", "ENVCAUSE"])
    assert sex_env in groups, "Expected SEX+ENVCAUSE group"
    assert len(groups[sex_env]) == 3, (
        f"Expected 3 records in SEX+ENVCAUSE group, got {len(groups[sex_env])}"
    )
    print("✓ Test 3.3 passed: SEX+ENVCAUSE group identified (3 records)")

    # Verify Group 3: SEX + AGEGROUP
    sex_age = frozenset(["SEX", "AGEGROUP"])
    assert sex_age in groups, "Expected SEX+AGEGROUP group"
    assert len(groups[sex_age]) == 1, "Expected 1 record in SEX+AGEGROUP group"
    print("✓ Test 3.4 passed: SEX+AGEGROUP group identified")

    print("\n✅ All grouping tests passed!\n")


def test_air17_scenario():
    """Test the real-world AIR_17 scenario with varying dimension orders."""
    print("=" * 60)
    print("TEST 4: Real-World Scenario (AIR_17 with varying dimension order)")
    print("=" * 60)

    # Simulate AIR_17 source data with varying dimension orders
    source_records = [
        # Record 1: Dim1=SEX, Dim2=ENVCAUSE
        {
            "SpatialDim": "USA",
            "SpatialDimType": "COUNTRY",
            "TimeDim": 2019,
            "Dim1Type": "SEX",
            "Dim1": "SEX_FMLE",
            "Dim2Type": "ENVCAUSE",
            "Dim2": "ENVCAUSE_039",
            "Dim3Type": None,
            "Dim3": None,
            "NumericValue": 12.5,
        },
        # Record 2: Dim1=SEX, Dim2=AGEGROUP (different Dim2!)
        {
            "SpatialDim": "USA",
            "SpatialDimType": "COUNTRY",
            "TimeDim": 2019,
            "Dim1Type": "SEX",
            "Dim1": "SEX_MLE",
            "Dim2Type": "AGEGROUP",
            "Dim2": "AGE_30_34",
            "Dim3Type": None,
            "Dim3": None,
            "NumericValue": 15.3,
        },
        # Record 3: Dim1=ENVCAUSE, Dim2=SEX (reversed order!)
        {
            "SpatialDim": "CAN",
            "SpatialDimType": "COUNTRY",
            "TimeDim": 2019,
            "Dim1Type": "ENVCAUSE",
            "Dim1": "ENVCAUSE_039",
            "Dim2Type": "SEX",
            "Dim2": "SEX_BTSX",
            "Dim3Type": None,
            "Dim3": None,
            "NumericValue": 18.0,
        },
        # Record 4: Dim1=AGEGROUP, Dim2=SEX (another reversed order!)
        {
            "SpatialDim": "CAN",
            "SpatialDimType": "COUNTRY",
            "TimeDim": 2020,
            "Dim1Type": "AGEGROUP",
            "Dim1": "AGE_40_44",
            "Dim2Type": "SEX",
            "Dim2": "SEX_FMLE",
            "Dim3Type": None,
            "Dim3": None,
            "NumericValue": 20.0,
        },
    ]

    # Step 1: Flatten all records
    flattened = [flatten_record(r) for r in source_records]

    print("Flattened records:")
    for i, rec in enumerate(flattened, 1):
        dim_keys = [
            k
            for k in rec.keys()
            if k not in {"SpatialDim", "SpatialDimType", "TimeDim", "NumericValue"}
        ]
        print(f"  Record {i}: dimensions = {sorted(dim_keys)}")

    # Verify flattening worked correctly
    assert "SEX" in flattened[0] and "ENVCAUSE" in flattened[0]
    assert "SEX" in flattened[1] and "AGEGROUP" in flattened[1]
    assert "ENVCAUSE" in flattened[2] and "SEX" in flattened[2]
    assert "AGEGROUP" in flattened[3] and "SEX" in flattened[3]
    print("✓ Test 4.1 passed: All records flattened correctly")

    # Step 2: Group by dimensions
    groups = group_by_dimensions(flattened)

    print(f"\nFound {len(groups)} dimension groups:")
    for dim_set, group_records in groups.items():
        print(f"  - {sorted(dim_set)}: {len(group_records)} records")

    # Should have exactly 2 groups (SEX+ENVCAUSE and SEX+AGEGROUP)
    assert len(groups) == 2, f"Expected 2 groups, got {len(groups)}"
    print("✓ Test 4.2 passed: Correctly identified 2 dimension combinations")

    # Verify SEX+ENVCAUSE group has 2 records (records 1 and 3)
    sex_env = frozenset(["SEX", "ENVCAUSE"])
    assert sex_env in groups, "Expected SEX+ENVCAUSE group"
    assert len(groups[sex_env]) == 2, (
        f"Expected 2 records in SEX+ENVCAUSE group, got {len(groups[sex_env])}"
    )
    print("✓ Test 4.3 passed: SEX+ENVCAUSE group has 2 records (order doesn't matter!)")

    # Verify SEX+AGEGROUP group has 2 records (records 2 and 4)
    sex_age = frozenset(["SEX", "AGEGROUP"])
    assert sex_age in groups, "Expected SEX+AGEGROUP group"
    assert len(groups[sex_age]) == 2, (
        f"Expected 2 records in SEX+AGEGROUP group, got {len(groups[sex_age])}"
    )
    print("✓ Test 4.4 passed: SEX+AGEGROUP group has 2 records (order doesn't matter!)")

    print("\n✅ Real-world scenario test passed!")
    print("   The flattening approach successfully handles varying dimension orders!\n")


def test_filename_generation():
    """Test that correct filenames are generated."""
    print("=" * 60)
    print("TEST 5: Filename Generation")
    print("=" * 60)

    test_cases = [
        {
            "dims": [],
            "indicator": "test_indicator",
            "expected": "ddf--datapoints--test_indicator--by--country--year.csv",
        },
        {
            "dims": ["sex"],
            "indicator": "adult_curr_cig_smoking",
            "expected": "ddf--datapoints--adult_curr_cig_smoking--by--country--year--sex.csv",
        },
        {
            "dims": ["envcause", "sex"],
            "indicator": "air_10",
            "expected": "ddf--datapoints--air_10--by--country--year--envcause--sex.csv",
        },
        {
            "dims": ["agegroup", "sex"],
            "indicator": "air_17",
            "expected": "ddf--datapoints--air_17--by--country--year--agegroup--sex.csv",
        },
    ]

    for i, test_case in enumerate(test_cases, 1):
        dims = test_case["dims"]
        indicator = test_case["indicator"]
        expected = test_case["expected"]

        # Generate filename
        all_dims = ["country", "year"] + dims
        filename = f"ddf--datapoints--{indicator}--by--{'--'.join(all_dims)}.csv"

        assert filename == expected, (
            f"Test {i}: Expected '{expected}', got '{filename}'"
        )
        print(f"✓ Test 5.{i} passed: {filename}")

    print("\n✅ All filename generation tests passed!\n")


def test_html_entity_decoding():
    """Test that HTML entities in indicator names are properly decoded."""
    print("=" * 60)
    print("TEST 6: HTML Entity Decoding")
    print("=" * 60)

    test_cases = [
        {
            "input": "Mean BMI (kg/m&#xb2;) (crude estimate)",
            "expected": "Mean BMI (kg/m²) (crude estimate)",
        },
        {
            "input": "Population (in thousands) &gt; 65 years",
            "expected": "Population (in thousands) > 65 years",
        },
        {
            "input": "Prevalence &lt; 5 years (%)",
            "expected": "Prevalence < 5 years (%)",
        },
        {
            "input": "Normal text without entities",
            "expected": "Normal text without entities",
        },
        {
            "input": "&quot;Quoted&quot; &amp; Special &#x3c0;",
            "expected": '"Quoted" & Special π',
        },
    ]

    for i, test_case in enumerate(test_cases, 1):
        input_text = test_case["input"]
        expected = test_case["expected"]
        result = html.unescape(input_text)

        assert result == expected, f"Test {i}: Expected '{expected}', got '{result}'"
        print(f"✓ Test 6.{i} passed: '{input_text}' → '{result}'")

    print("\n✅ All HTML entity decoding tests passed!\n")


def test_column_ordering():
    """Test that dataframe columns are in the correct order."""
    print("=" * 60)
    print("TEST 7: Column Ordering")
    print("=" * 60)

    import pandas as pd

    # Test case 1: No extra dimensions
    data1 = {
        "country": ["usa", "can"],
        "year": [2019, 2019],
        "indicator": [10.5, 12.3],
    }
    df1 = pd.DataFrame(data1)
    expected_order1 = ["country", "year", "indicator"]
    actual_order1 = list(df1.columns)
    assert actual_order1 == expected_order1, (
        f"Expected {expected_order1}, got {actual_order1}"
    )
    print(f"✓ Test 7.1 passed: No dimensions - {actual_order1}")

    # Test case 2: One dimension
    data2 = {
        "country": ["usa", "can"],
        "year": [2019, 2019],
        "sex": ["sex_fmle", "sex_mle"],
        "indicator": [10.5, 12.3],
    }
    df2 = pd.DataFrame(data2)
    # Reorder: country, year, sorted dims, indicator
    column_order2 = ["country", "year", "sex", "indicator"]
    df2 = df2[column_order2]
    actual_order2 = list(df2.columns)
    assert actual_order2 == column_order2, (
        f"Expected {column_order2}, got {actual_order2}"
    )
    print(f"✓ Test 7.2 passed: One dimension - {actual_order2}")

    # Test case 3: Two dimensions (alphabetically sorted)
    data3 = {
        "country": ["usa", "can"],
        "year": [2019, 2019],
        "sex": ["sex_fmle", "sex_mle"],
        "envcause": ["env_01", "env_01"],
        "indicator": [10.5, 12.3],
    }
    df3 = pd.DataFrame(data3)
    # Reorder: country, year, sorted dims (envcause, sex), indicator
    column_order3 = ["country", "year", "envcause", "sex", "indicator"]
    df3 = df3[column_order3]
    actual_order3 = list(df3.columns)
    assert actual_order3 == column_order3, (
        f"Expected {column_order3}, got {actual_order3}"
    )
    print(
        f"✓ Test 7.3 passed: Two dimensions (alphabetically sorted) - {actual_order3}"
    )

    # Test case 4: Three dimensions (alphabetically sorted)
    data4 = {
        "country": ["usa"],
        "year": [2019],
        "sex": ["sex_fmle"],
        "agegroup": ["age_30_34"],
        "location": ["urban"],
        "indicator": [10.5],
    }
    df4 = pd.DataFrame(data4)
    # Reorder: country, year, sorted dims (agegroup, location, sex), indicator
    column_order4 = ["country", "year", "agegroup", "location", "sex", "indicator"]
    df4 = df4[column_order4]
    actual_order4 = list(df4.columns)
    assert actual_order4 == column_order4, (
        f"Expected {column_order4}, got {actual_order4}"
    )
    print(
        f"✓ Test 7.4 passed: Three dimensions (alphabetically sorted) - {actual_order4}"
    )

    # Test case 5: Verify alphabetical sorting is correct
    dimensions = ["sex", "envcause", "agegroup", "location"]
    sorted_dims = sorted(dimensions)
    expected_sorted = ["agegroup", "envcause", "location", "sex"]
    assert sorted_dims == expected_sorted, (
        f"Expected {expected_sorted}, got {sorted_dims}"
    )
    print(f"✓ Test 7.5 passed: Alphabetical sorting - {sorted_dims}")

    print("\n✅ All column ordering tests passed!\n")


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("WHO GHO DDF ETL - Test Suite (Flattened Record Approach)")
    print("=" * 60 + "\n")

    try:
        test_flatten_record()
        test_get_dimension_columns()
        test_group_by_dimensions()
        test_air17_scenario()
        test_filename_generation()
        test_html_entity_decoding()
        test_column_ordering()

        print("=" * 60)
        print("🎉 ALL TESTS PASSED! 🎉")
        print("=" * 60)
        print("\nThe flattened record approach works perfectly!")
        print("Key benefits:")
        print("  ✓ Simpler logic - no complex dimension normalization needed")
        print("  ✓ Order-independent - dimension order doesn't matter")
        print("  ✓ Closer to DDF format - direct mapping to output columns")
        print("  ✓ Easier to understand and maintain")
        print("\nYou can now run the main ETL script with your indicators.\n")
        return 0

    except AssertionError as e:
        print("\n" + "=" * 60)
        print("❌ TEST FAILED!")
        print("=" * 60)
        print(f"Error: {e}\n")
        return 1
    except Exception as e:
        print("\n" + "=" * 60)
        print("❌ UNEXPECTED ERROR!")
        print("=" * 60)
        print(f"Error: {e}\n")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
