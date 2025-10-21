# WHO GHO DDF ETL Script

This ETL (Extract, Transform, Load) script processes WHO Global Health Observatory (GHO) data and converts it into DDF (Data Description Format) for use with Gapminder tools.

## How to run the script

### Step 1: Define Indicators to Process

Edit the `INDICATORS_CONFIG` dictionary in `etl.py`:

```python
INDICATORS_CONFIG = {
    # Example: Indicator with no extra dimensions (country/year only)
    "SA_0000001700": {},
    
    # Example: Indicator with SEX dimension
    "Adult_curr_cig_smoking": {},
    
    # Example: Indicator with SEX and ENVCAUSE dimensions
    "AIR_10": {},
    
    # Example: Indicator with varying dimensions (SEX+ENVCAUSE or SEX+AGEGROUP)
    "AIR_17": {},
    
    # Add more indicators here
}
```

The keys should match the JSON filenames in `etl/source/` (without the `.json` extension).

Currently we don't support any additional configurations other than selecting indicators.

### Step 2: Ensure Source Files Exist

Make sure you have downloaded the indicator data files to `etl/source/` by running the update_source.py script.

```bash
cd etl/script
python update_source.py
```

### Step 3: Run the Script

```bash
cd etl/script
python etl.py
```

