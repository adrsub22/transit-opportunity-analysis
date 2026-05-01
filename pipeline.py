"""
Transit Opportunity Analysis — block-group-level pipeline.

Pulls ACS 5-year demographics, ACS disability (tract-level, joined to BGs),
ACS age-by-sex, LEHD LODES employment, and TIGER block-group geometry, and
produces a multi-year panel plus a current snapshot with derived density,
growth, and composite transit-planning indices.

Notes on the disability join:
    ACS Table B18101 is not published at block-group geography in Texas.
    The pipeline pulls B18101 at the tract level and joins the tract-level
    rate to every block group within the tract. All BGs in the same tract
    will share identical disability values. This is documented in the
    output Glossary tab.

Configuration is at the top of the file. Set CENSUS_API_KEY as an
environment variable before running:

    export CENSUS_API_KEY="your_key_here"   # macOS / Linux
    setx   CENSUS_API_KEY "your_key_here"   # Windows
"""

import os
import requests
import gzip
import io
import zipfile
import glob
import numpy as np
import pandas as pd
import geopandas as gpd
from datetime import datetime

# -----------------------------
# Configuration
# -----------------------------
api_key = os.getenv("CENSUS_API_KEY")
if not api_key:
    raise RuntimeError(
        "CENSUS_API_KEY environment variable not set. "
        "Get a free key at https://api.census.gov/data/key_signup.html"
    )

# Example study area: Bexar County, TX (FIPS 48029).
# To run elsewhere, change state_fips and counties_fips, and update the
# TIGER URL and CRS at the bottom of this file to match the new state.
counties_fips = ['48029']
state_fips    = '48'

lodes_years = [2023, 2019]
acs_years   = [2024, 2019]

# ACS 5-year vintages with end years <= this threshold are published on
# 2010 BG geography and need to be harmonized onto 2020 BG geography
# before being compared to later vintages. See docs/harmonization.md.
PRE_2020_VINTAGE_THRESHOLD = 2020
HARMONIZE_PRE_2020_BG      = True

output_folder = "outputs"
os.makedirs(output_folder, exist_ok=True)

# Harmonization module (sits next to pipeline.py)
from harmonize_bg import build_bg_crosswalk, harmonize_counts, validate_crosswalk

# -----------------------------
# Download LODES (jobs by workplace)
# -----------------------------
def download_lodes(year):

    url = f"https://lehd.ces.census.gov/data/lodes/LODES8/tx/wac/tx_wac_S000_JT00_{year}.csv.gz"
    print(f"Downloading LODES {year}")

    r = requests.get(url)

    if r.status_code != 200:
        return pd.DataFrame()

    with gzip.open(io.BytesIO(r.content), 'rt') as f:
        df = pd.read_csv(f, dtype={'w_geocode': str})

    df = df[df['w_geocode'].str.startswith(tuple(counties_fips))]

    df = df[['w_geocode', 'C000', 'CE01', 'CE02', 'CE03']]

    df.columns = [
        'GEOID20',
        'Jobs',
        'JobsLess1250',
        'Jobs1251_3333',
        'JobsOver3333'
    ]

    # Aggregate from block (15-digit) to block group (12-digit)
    df['BGGEOID']    = df['GEOID20'].str[:12]
    df['lodes_year'] = year
    df['lodes_date'] = pd.to_datetime(f"{year}-01-01")

    df = df.groupby(['BGGEOID', 'lodes_year', 'lodes_date'], as_index=False).sum(numeric_only=True)

    return df

# -----------------------------
# Fetch ACS demographics, income, poverty, transit commute (block group)
# -----------------------------
def fetch_acs(year):

    print(f"Fetching ACS {year}")

    variables = [
        'B01003_001E',  # total population
        'B02001_002E',  # white alone
        'B08301_001E',  # workers 16+
        'B08301_010E',  # transit commuters
        'B19013_001E',  # median household income
        'C17002_001E',  # poverty universe
        'C17002_002E',  # below 0.5 of poverty
        'C17002_003E',  # 0.5 to 0.99 of poverty
    ]

    geo = f"&for=block group:*&in=state:{state_fips}&in=county:*&in=tract:*"

    url = f"https://api.census.gov/data/{year}/acs/acs5?get={','.join(variables)}{geo}&key={api_key}"

    r = requests.get(url)

    if r.status_code != 200:
        return pd.DataFrame()

    data = r.json()

    df = pd.DataFrame(data[1:], columns=data[0])

    df['BGGEOID']  = df['state'] + df['county'] + df['tract'] + df['block group']
    df['acs_year'] = year
    df['acs_date'] = pd.to_datetime(f"{year}-01-01")

    rename = {
        'B01003_001E': 'Tot_Pop',
        'B02001_002E': 'TotWhtPop',
        'B08301_001E': 'Work_Pop',
        'B08301_010E': 'Pub_Transit',
        'B19013_001E': 'MHI',
        'C17002_001E': 'PovStatDet',
        'C17002_002E': 'Less50Pov',
        'C17002_003E': '50to99Pov',
    }

    df = df.rename(columns=rename)

    numeric = list(rename.values())
    df[numeric] = df[numeric].apply(pd.to_numeric, errors='coerce').fillna(0)

    df['TotMinPop']  = df['Tot_Pop'] - df['TotWhtPop']
    df['PovLess100'] = df['Less50Pov'] + df['50to99Pov']

    df['MinPopPer']  = np.where(df['Tot_Pop']    > 0, (df['TotMinPop']  / df['Tot_Pop'])    * 100, 0)
    df['AtBelowPov'] = np.where(df['PovStatDet'] > 0, (df['PovLess100'] / df['PovStatDet']) * 100, 0)
    df['Transit%']   = np.where(df['Work_Pop']   > 0, (df['Pub_Transit'] / df['Work_Pop'])  * 100, 0)

    return df

# -----------------------------
# Fetch age / sex at block group level
# B01001 — Sex by Age
# (separate call to stay under the Census API 50-variable limit)
# -----------------------------
def fetch_age_sex(year):

    print(f"Fetching age/sex B01001 (block group) {year}")

    # B01001_002E = Male total ... B01001_049E = Female 85+
    variables = [f'B01001_{str(i).zfill(3)}E' for i in range(2, 50)]

    geo = f"&for=block group:*&in=state:{state_fips}&in=county:*&in=tract:*"

    url = f"https://api.census.gov/data/{year}/acs/acs5?get={','.join(variables)}{geo}&key={api_key}"

    r = requests.get(url)

    if r.status_code != 200:
        print(f"  WARNING: B01001 {year} returned {r.status_code}")
        return pd.DataFrame()

    data = r.json()

    df = pd.DataFrame(data[1:], columns=data[0])

    df['BGGEOID']     = df['state'] + df['county'] + df['tract'] + df['block group']
    df['agesex_year'] = year

    for v in variables:
        df[v] = pd.to_numeric(df[v], errors='coerce').fillna(0)

    # Male totals and age groups
    df['Male_Tot']    = df['B01001_002E']
    df['Male_Under5'] = df['B01001_003E']
    df['Male_5to17']  = df['B01001_004E'] + df['B01001_005E'] + df['B01001_006E']
    df['Male_18to44'] = (
        df['B01001_007E'] + df['B01001_008E'] + df['B01001_009E'] +
        df['B01001_010E'] + df['B01001_011E'] + df['B01001_012E'] +
        df['B01001_013E'] + df['B01001_014E']
    )
    df['Male_45to64'] = (
        df['B01001_015E'] + df['B01001_016E'] + df['B01001_017E'] +
        df['B01001_018E'] + df['B01001_019E']
    )
    df['Male_65Plus'] = (
        df['B01001_020E'] + df['B01001_021E'] + df['B01001_022E'] +
        df['B01001_023E'] + df['B01001_024E'] + df['B01001_025E']
    )

    # Female totals and age groups
    df['Female_Tot']    = df['B01001_026E']
    df['Female_Under5'] = df['B01001_027E']
    df['Female_5to17']  = df['B01001_028E'] + df['B01001_029E'] + df['B01001_030E']
    df['Female_18to44'] = (
        df['B01001_031E'] + df['B01001_032E'] + df['B01001_033E'] +
        df['B01001_034E'] + df['B01001_035E'] + df['B01001_036E'] +
        df['B01001_037E'] + df['B01001_038E']
    )
    df['Female_45to64'] = (
        df['B01001_039E'] + df['B01001_040E'] + df['B01001_041E'] +
        df['B01001_042E'] + df['B01001_043E']
    )
    df['Female_65Plus'] = (
        df['B01001_044E'] + df['B01001_045E'] + df['B01001_046E'] +
        df['B01001_047E'] + df['B01001_048E'] + df['B01001_049E']
    )

    # Combined totals
    df['Tot_Under5'] = df['Male_Under5'] + df['Female_Under5']
    df['Tot_5to17']  = df['Male_5to17']  + df['Female_5to17']
    df['Tot_18to44'] = df['Male_18to44'] + df['Female_18to44']
    df['Tot_45to64'] = df['Male_45to64'] + df['Female_45to64']
    df['Tot_65Plus'] = df['Male_65Plus'] + df['Female_65Plus']

    # Percentages of total population
    tot = df['Male_Tot'] + df['Female_Tot']

    for grp in ['Under5', '5to17', '18to44', '45to64', '65Plus']:
        df[f'Male_{grp}_Pct']   = np.where(tot > 0, (df[f'Male_{grp}']   / tot) * 100, 0)
        df[f'Female_{grp}_Pct'] = np.where(tot > 0, (df[f'Female_{grp}'] / tot) * 100, 0)
        df[f'Tot_{grp}_Pct']    = np.where(tot > 0, (df[f'Tot_{grp}']    / tot) * 100, 0)

    df['Male_Tot_Pct']   = np.where(tot > 0, (df['Male_Tot']   / tot) * 100, 0)
    df['Female_Tot_Pct'] = np.where(tot > 0, (df['Female_Tot'] / tot) * 100, 0)

    # Keep only derived columns
    keep_cols = [
        'BGGEOID', 'agesex_year',
        'Male_Tot', 'Female_Tot',
        'Male_Under5', 'Male_5to17', 'Male_18to44', 'Male_45to64', 'Male_65Plus',
        'Female_Under5', 'Female_5to17', 'Female_18to44', 'Female_45to64', 'Female_65Plus',
        'Tot_Under5', 'Tot_5to17', 'Tot_18to44', 'Tot_45to64', 'Tot_65Plus',
        'Male_Tot_Pct', 'Female_Tot_Pct',
        'Male_Under5_Pct', 'Male_5to17_Pct', 'Male_18to44_Pct', 'Male_45to64_Pct', 'Male_65Plus_Pct',
        'Female_Under5_Pct', 'Female_5to17_Pct', 'Female_18to44_Pct', 'Female_45to64_Pct', 'Female_65Plus_Pct',
        'Tot_Under5_Pct', 'Tot_5to17_Pct', 'Tot_18to44_Pct', 'Tot_45to64_Pct', 'Tot_65Plus_Pct',
    ]

    return df[keep_cols]

# -----------------------------
# Fetch disability at TRACT level
# B18101 — Sex by Age by Disability Status
# Not published at block group level in Texas; tract rate is applied to all
# block groups within the tract.
# -----------------------------
def fetch_disability_tract(year):

    print(f"Fetching disability B18101 (tract) {year}")

    variables = [
        'B18101_001E',
        'B18101_004E', 'B18101_007E', 'B18101_010E', 'B18101_013E', 'B18101_016E', 'B18101_019E',
        'B18101_023E', 'B18101_026E', 'B18101_029E', 'B18101_032E', 'B18101_035E', 'B18101_038E',
    ]

    geo = f"&for=tract:*&in=state:{state_fips}&in=county:*"

    url = f"https://api.census.gov/data/{year}/acs/acs5?get={','.join(variables)}{geo}&key={api_key}"

    r = requests.get(url)

    if r.status_code != 200:
        return pd.DataFrame()

    data = r.json()

    df = pd.DataFrame(data[1:], columns=data[0])

    df['TRACTGEOID'] = df['state'] + df['county'] + df['tract']

    for v in variables:
        df[v] = pd.to_numeric(df[v], errors='coerce').fillna(0)

    df['DisabUniverse'] = df['B18101_001E']

    df['DisabUnder18'] = (
        df['B18101_004E'] + df['B18101_007E'] +
        df['B18101_023E'] + df['B18101_026E']
    )

    df['Disab18to64'] = (
        df['B18101_010E'] + df['B18101_013E'] +
        df['B18101_029E'] + df['B18101_032E']
    )

    df['Disab65Plus'] = (
        df['B18101_016E'] + df['B18101_019E'] +
        df['B18101_035E'] + df['B18101_038E']
    )

    df['TotDisab'] = df['DisabUnder18'] + df['Disab18to64'] + df['Disab65Plus']

    df['DisabPct']        = np.where(df['DisabUniverse'] > 0, (df['TotDisab']     / df['DisabUniverse']) * 100, 0)
    df['DisabUnder18Pct'] = np.where(df['DisabUniverse'] > 0, (df['DisabUnder18'] / df['DisabUniverse']) * 100, 0)
    df['Disab18to64Pct']  = np.where(df['DisabUniverse'] > 0, (df['Disab18to64']  / df['DisabUniverse']) * 100, 0)
    df['Disab65PlusPct']  = np.where(df['DisabUniverse'] > 0, (df['Disab65Plus']  / df['DisabUniverse']) * 100, 0)

    df['disab_year'] = year

    keep = [
        'TRACTGEOID', 'disab_year',
        'DisabUniverse', 'DisabUnder18', 'Disab18to64', 'Disab65Plus',
        'TotDisab', 'DisabPct',
        'DisabUnder18Pct', 'Disab18to64Pct', 'Disab65PlusPct',
    ]

    return df[keep]

# -----------------------------
# Pull data
# -----------------------------
lodes_df  = pd.concat([download_lodes(y)        for y in lodes_years], ignore_index=True)
acs_df    = pd.concat([fetch_acs(y)             for y in acs_years],   ignore_index=True)
disab_df  = pd.concat([fetch_disability_tract(y) for y in acs_years],   ignore_index=True)
agesex_df = pd.concat([fetch_age_sex(y)         for y in acs_years],   ignore_index=True)

acs_df    = acs_df.drop_duplicates(['BGGEOID',     'acs_year'])
lodes_df  = lodes_df.drop_duplicates(['BGGEOID',   'lodes_year'])
disab_df  = disab_df.drop_duplicates(['TRACTGEOID', 'disab_year'])
agesex_df = agesex_df.drop_duplicates(['BGGEOID',  'agesex_year'])

# -----------------------------
# Harmonize pre-2020 ACS data onto 2020 geography
#
# ACS 5-year vintages with end years <= 2020 use 2010 BG and tract
# boundaries; vintages from 2021 onward use 2020 boundaries. Joining
# across this break on GEOID silently mismatches split / merged /
# renumbered units. We apportion the older counts onto 2020 geography
# using the Census Bureau's official relationship files.
# Both BGs and tracts are harmonized so the disability join stays
# vintage-consistent. See docs/harmonization.md.
# -----------------------------
if HARMONIZE_PRE_2020_BG and any(y <= PRE_2020_VINTAGE_THRESHOLD for y in acs_years):

    print("Harmonizing pre-2020 ACS vintages onto 2020 geography")

    bg_crosswalk    = build_bg_crosswalk(state_fips=state_fips)
    tract_crosswalk = build_tract_crosswalk(state_fips=state_fips)

    validate_crosswalk(bg_crosswalk)
    validate_crosswalk(tract_crosswalk)

    bg_count_cols = [
        'Tot_Pop', 'TotWhtPop', 'Work_Pop', 'Pub_Transit',
        'PovStatDet', 'Less50Pov', '50to99Pov',
    ]

    # MHI is a median, not a count — it cannot be apportioned correctly.
    # We approximate it as a population-weighted average of the source-BG
    # medians: for each 2020 BG, we sum (MHI × Tot_Pop × weight) across
    # contributing 2010 BGs and divide by the sum of (Tot_Pop × weight).
    # This is a defensible approximation, NOT a true median, and is flagged
    # in the data dictionary and Glossary tab as such. A true median would
    # require the underlying household income distribution, which ACS does
    # not publish at BG level.

    agesex_count_cols = [
        'Male_Tot', 'Female_Tot',
        'Male_Under5', 'Male_5to17', 'Male_18to44', 'Male_45to64', 'Male_65Plus',
        'Female_Under5', 'Female_5to17', 'Female_18to44', 'Female_45to64', 'Female_65Plus',
        'Tot_Under5', 'Tot_5to17', 'Tot_18to44', 'Tot_45to64', 'Tot_65Plus',
    ]

    disab_count_cols = [
        'DisabUniverse', 'DisabUnder18', 'Disab18to64', 'Disab65Plus', 'TotDisab',
    ]

    def _harmonize_acs_year(sub):
        """Apportion ACS counts then recompute the derived percentages."""

        # Compute the MHI weighted-average BEFORE we run harmonize_counts,
        # because we need the source MHI and source Tot_Pop in the same
        # row. We do it by hand, in parallel to the count apportionment.
        sub = sub.copy()
        sub['BGGEOID'] = sub['BGGEOID'].astype(str)
        mhi_src = sub.merge(bg_crosswalk, left_on='BGGEOID', right_on='GEOID_2010', how='inner')
        mhi_src['MHI']     = pd.to_numeric(mhi_src['MHI'],     errors='coerce')
        mhi_src['Tot_Pop'] = pd.to_numeric(mhi_src['Tot_Pop'], errors='coerce')
        mhi_src['_w']      = mhi_src['Tot_Pop'].fillna(0) * mhi_src['weight']
        mhi_src['_wMHI']   = mhi_src['MHI'].fillna(0)     * mhi_src['_w']

        mhi_agg = (
            mhi_src.groupby('GEOID_2020', as_index=False)
                   .agg(_w=('_w', 'sum'), _wMHI=('_wMHI', 'sum'))
        )
        # Safe division: NaN where _w is 0. Avoids the numexpr 0/0
        # ZeroDivisionError that np.where doesn't actually guard against
        # (np.where evaluates both branches before applying the condition).
        mhi_agg['MHI'] = mhi_agg['_wMHI'] / mhi_agg['_w'].replace(0, np.nan)
        mhi_agg = mhi_agg[['GEOID_2020', 'MHI']].rename(columns={'GEOID_2020': 'BGGEOID'})

        # Apportion the count variables.
        out = harmonize_counts(sub, bg_crosswalk, bg_count_cols, geoid_col='BGGEOID')

        # Recompute derived percentages from harmonized counts.
        # `.replace(0, np.nan)` on denominators avoids the numexpr 0/0 issue;
        # the np.where guard then produces 0 for those rows as before.
        out['TotMinPop']  = out['Tot_Pop']    - out['TotWhtPop']
        out['PovLess100'] = out['Less50Pov']  + out['50to99Pov']

        out['MinPopPer']  = np.where(out['Tot_Pop']    > 0, (out['TotMinPop']   / out['Tot_Pop'].replace(0, np.nan))    * 100, 0)
        out['AtBelowPov'] = np.where(out['PovStatDet'] > 0, (out['PovLess100']  / out['PovStatDet'].replace(0, np.nan)) * 100, 0)
        out['Transit%']   = np.where(out['Work_Pop']   > 0, (out['Pub_Transit'] / out['Work_Pop'].replace(0, np.nan))   * 100, 0)

        # Attach the population-weighted MHI approximation.
        out = out.merge(mhi_agg, on='BGGEOID', how='left')

        out['acs_year'] = sub['acs_year'].iloc[0]
        out['acs_date'] = sub['acs_date'].iloc[0]
        return out

    def _harmonize_agesex_year(sub):
        """Apportion age/sex counts then recompute percentages."""
        out = harmonize_counts(sub, bg_crosswalk, agesex_count_cols, geoid_col='BGGEOID')

        tot      = out['Male_Tot'] + out['Female_Tot']
        tot_safe = tot.replace(0, np.nan)  # safe denominator for divisions

        for grp in ['Under5', '5to17', '18to44', '45to64', '65Plus']:
            out[f'Male_{grp}_Pct']   = np.where(tot > 0, (out[f'Male_{grp}']   / tot_safe) * 100, 0)
            out[f'Female_{grp}_Pct'] = np.where(tot > 0, (out[f'Female_{grp}'] / tot_safe) * 100, 0)
            out[f'Tot_{grp}_Pct']    = np.where(tot > 0, (out[f'Tot_{grp}']    / tot_safe) * 100, 0)

        out['Male_Tot_Pct']   = np.where(tot > 0, (out['Male_Tot']   / tot_safe) * 100, 0)
        out['Female_Tot_Pct'] = np.where(tot > 0, (out['Female_Tot'] / tot_safe) * 100, 0)

        out['agesex_year'] = sub['agesex_year'].iloc[0]
        return out

    def _harmonize_disab_year(sub):
        """Apportion tract-level disability counts and recompute rates."""
        out = harmonize_counts(sub, tract_crosswalk, disab_count_cols, geoid_col='TRACTGEOID')

        univ_safe = out['DisabUniverse'].replace(0, np.nan)

        out['DisabPct']        = np.where(out['DisabUniverse'] > 0, (out['TotDisab']     / univ_safe) * 100, 0)
        out['DisabUnder18Pct'] = np.where(out['DisabUniverse'] > 0, (out['DisabUnder18'] / univ_safe) * 100, 0)
        out['Disab18to64Pct']  = np.where(out['DisabUniverse'] > 0, (out['Disab18to64']  / univ_safe) * 100, 0)
        out['Disab65PlusPct']  = np.where(out['DisabUniverse'] > 0, (out['Disab65Plus']  / univ_safe) * 100, 0)

        out['disab_year'] = sub['disab_year'].iloc[0]
        return out

    # Apply per vintage; pass post-2020 vintages through unchanged.
    acs_df = pd.concat([
        _harmonize_acs_year(sub) if yr <= PRE_2020_VINTAGE_THRESHOLD else sub
        for yr, sub in acs_df.groupby('acs_year')
    ], ignore_index=True)

    agesex_df = pd.concat([
        _harmonize_agesex_year(sub) if yr <= PRE_2020_VINTAGE_THRESHOLD else sub
        for yr, sub in agesex_df.groupby('agesex_year')
    ], ignore_index=True)

    disab_df = pd.concat([
        _harmonize_disab_year(sub) if yr <= PRE_2020_VINTAGE_THRESHOLD else sub
        for yr, sub in disab_df.groupby('disab_year')
    ], ignore_index=True)

# -----------------------------
# Join tract disability -> block groups
# -----------------------------
acs_df['TRACTGEOID'] = acs_df['BGGEOID'].str[:11]

acs_df = acs_df.merge(
    disab_df,
    left_on  = ['TRACTGEOID', 'acs_year'],
    right_on = ['TRACTGEOID', 'disab_year'],
    how      = 'left'
)

disab_cols = [
    'DisabUniverse', 'DisabUnder18', 'Disab18to64', 'Disab65Plus',
    'TotDisab', 'DisabPct',
    'DisabUnder18Pct', 'Disab18to64Pct', 'Disab65PlusPct',
]
acs_df[disab_cols] = acs_df[disab_cols].fillna(0)

# -----------------------------
# Join age / sex -> block groups
# -----------------------------
acs_df = acs_df.merge(
    agesex_df,
    left_on  = ['BGGEOID', 'acs_year'],
    right_on = ['BGGEOID', 'agesex_year'],
    how      = 'left'
)

agesex_num_cols = [c for c in agesex_df.columns if c not in ['BGGEOID', 'agesex_year']]
acs_df[agesex_num_cols] = acs_df[agesex_num_cols].fillna(0)

# -----------------------------
# Baseline comparison panel (matched ACS / LODES years)
# -----------------------------
common_years = sorted(set(acs_df['acs_year']).intersection(lodes_df['lodes_year']))

base_rows = []

for yr in common_years:

    acs_y   = acs_df[acs_df['acs_year']     == yr]
    lodes_y = lodes_df[lodes_df['lodes_year'] == yr]

    merged = acs_y.merge(
        lodes_y[['BGGEOID', 'Jobs', 'JobsLess1250', 'Jobs1251_3333', 'JobsOver3333']],
        on  = 'BGGEOID',
        how = 'left'
    )

    merged['dataset_type'] = "baseline_comparison"
    merged['lodes_year']   = yr
    merged['lodes_date']   = pd.to_datetime(f"{yr}-01-01")

    base_rows.append(merged)

base_panel = pd.concat(base_rows, ignore_index=True)

# -----------------------------
# Current snapshot (latest ACS + latest LODES)
# -----------------------------
acs_latest   = acs_df['acs_year'].max()
lodes_latest = lodes_df['lodes_year'].max()

snapshot = acs_df[acs_df['acs_year'] == acs_latest].merge(
    lodes_df[lodes_df['lodes_year'] == lodes_latest],
    on  = 'BGGEOID',
    how = 'left'
)

snapshot['dataset_type'] = "current_snapshot"

combined = pd.concat([base_panel, snapshot], ignore_index=True)

# -----------------------------
# Load TIGER block-group geometry
# (Texas state file; substitute the appropriate state file for other regions)
# -----------------------------
tiger_url = "https://www2.census.gov/geo/tiger/TIGER2024/BG/tl_2024_48_bg.zip"

zip_name = "tl_2024_48_bg.zip"
folder   = "tl_2024_48_bg"

if not os.path.exists(zip_name):
    with open(zip_name, 'wb') as f:
        f.write(requests.get(tiger_url).content)

if not os.path.exists(folder):
    with zipfile.ZipFile(zip_name, 'r') as z:
        z.extractall(folder)

shp = glob.glob(os.path.join(folder, "*.shp"))[0]

gdf = gpd.read_file(shp)
gdf = gdf[gdf['COUNTYFP'].isin([c[2:] for c in counties_fips])]
gdf = gdf.rename(columns={'GEOID': 'BGGEOID'})

# -----------------------------
# Merge geometry
# -----------------------------
final_gdf = gdf.merge(combined, on='BGGEOID', how='left')

# -----------------------------
# Density calculations (reproject to equal-area for accurate area)
# EPSG:3081 = Texas Centric Albers Equal Area.
# For other regions, substitute an appropriate equal-area projection
# (e.g. EPSG:5070 for CONUS).
# -----------------------------
final_gdf = final_gdf.to_crs(epsg=3081)

final_gdf['Area_sqm']   = final_gdf.geometry.area
final_gdf['Area_acres'] = final_gdf['Area_sqm'] / 4046.856

final_gdf['Pop_per_acre']  = np.where(final_gdf['Area_acres'] > 0, final_gdf['Tot_Pop'] / final_gdf['Area_acres'].replace(0, np.nan), 0)
final_gdf['Jobs_per_acre'] = np.where(final_gdf['Area_acres'] > 0, final_gdf['Jobs']    / final_gdf['Area_acres'].replace(0, np.nan), 0)

final_gdf['pop_job_den'] = final_gdf['Pop_per_acre'] + final_gdf['Jobs_per_acre']

# -----------------------------
# Growth metrics
# -----------------------------
baseline = (
    final_gdf[final_gdf['dataset_type'] == "baseline_comparison"]
    [['BGGEOID', 'Tot_Pop', 'Jobs']]
    .rename(columns={'Tot_Pop': 'baseline_pop', 'Jobs': 'baseline_jobs'})
)

current = (
    final_gdf[final_gdf['dataset_type'] == "current_snapshot"]
    [['BGGEOID', 'Tot_Pop', 'Jobs']]
    .rename(columns={'Tot_Pop': 'current_pop', 'Jobs': 'current_jobs'})
)

growth = baseline.merge(current, on='BGGEOID')

growth['Pop_Growth']  = growth['current_pop']  - growth['baseline_pop']
growth['Jobs_Growth'] = growth['current_jobs'] - growth['baseline_jobs']

growth['Pop_Growth_Pct']  = np.where(growth['baseline_pop']  > 0, (growth['Pop_Growth']  / growth['baseline_pop'].replace(0, np.nan))  * 100, 0)
growth['Jobs_Growth_Pct'] = np.where(growth['baseline_jobs'] > 0, (growth['Jobs_Growth'] / growth['baseline_jobs'].replace(0, np.nan)) * 100, 0)

final_gdf = final_gdf.merge(
    growth[['BGGEOID', 'Pop_Growth', 'Pop_Growth_Pct', 'Jobs_Growth', 'Jobs_Growth_Pct']],
    on  = 'BGGEOID',
    how = 'left'
)

# -----------------------------
# Growth categories
# -----------------------------
final_gdf["Growth_Category"] = pd.cut(
    final_gdf["Pop_Growth_Pct"],
    bins   = [-999, -5, 0, 5, 20, 999],
    labels = [
        "1 Population Decline",
        "2 Stable",
        "3 Slow Growth",
        "4 Moderate Growth",
        "5 Rapid Growth",
    ]
).astype(str)

final_gdf["Combined_Growth_Index"] = (
    final_gdf["Pop_Growth_Pct"] * 0.6 +
    final_gdf["Jobs_Growth_Pct"] * 0.4
)

# -----------------------------
# Composite indices
# -----------------------------
final_gdf['Transit_Dependency_Index'] = (
    final_gdf['AtBelowPov'] * 0.30 +
    final_gdf['MinPopPer']  * 0.25 +
    final_gdf['Transit%']   * 0.25 +
    final_gdf['DisabPct']   * 0.20
)

final_gdf['Transit_Supportive_Density_Index'] = final_gdf['pop_job_den'] * 10

final_gdf['Jobs_Housing_Ratio'] = np.where(final_gdf['Tot_Pop'] > 0, final_gdf['Jobs'] / final_gdf['Tot_Pop'].replace(0, np.nan), 0)

final_gdf['Jobs_Housing_Balance_Score'] = 1 - abs(1 - final_gdf['Jobs_Housing_Ratio'])

final_gdf['Transit_Opportunity_Index'] = (
    final_gdf['Transit_Dependency_Index']         * 0.4 +
    final_gdf['Transit_Supportive_Density_Index'] * 0.3 +
    final_gdf['Pop_Growth_Pct']                   * 0.2 +
    final_gdf['Jobs_Growth_Pct']                  * 0.1
)

# -----------------------------
# Drop helper columns
# -----------------------------
for col in ['GEOID20', 'TRACTGEOID', 'disab_year', 'agesex_year']:
    if col in final_gdf.columns:
        final_gdf = final_gdf.drop(columns=[col])

# -----------------------------
# Current snapshot view
# -----------------------------
current_gdf = final_gdf[final_gdf["dataset_type"] == "current_snapshot"].copy()

# -----------------------------
# Block-group summary (baseline vs current, side by side)
# -----------------------------
summary_df = final_gdf.pivot_table(
    index   = "BGGEOID",
    values  = ["Tot_Pop", "Jobs", "Transit_Opportunity_Index"],
    columns = "dataset_type",
    aggfunc = "first"
)

summary_df.columns = ["_".join(col).strip() for col in summary_df.columns.values]
summary_df = summary_df.reset_index()

summary_df = summary_df.merge(
    growth[['BGGEOID', 'Pop_Growth', 'Pop_Growth_Pct', 'Jobs_Growth', 'Jobs_Growth_Pct']],
    on  = "BGGEOID",
    how = "left"
)

summary_df = summary_df[[
    "BGGEOID",
    "Tot_Pop_baseline_comparison",
    "Tot_Pop_current_snapshot",
    "Pop_Growth",
    "Pop_Growth_Pct",
    "Jobs_baseline_comparison",
    "Jobs_current_snapshot",
    "Jobs_Growth",
    "Jobs_Growth_Pct",
    "Transit_Opportunity_Index_baseline_comparison",
    "Transit_Opportunity_Index_current_snapshot",
]]

# -----------------------------
# Glossary (ships with the Excel output)
# -----------------------------
glossary = pd.DataFrame([

    ("MHI",
     """Median household income (ACS Table B19013_001E).

For 2020+ ACS vintages: published directly at block group level.

For pre-2020 vintages (which use 2010 BG geography), MHI on
harmonized rows is a population-weighted AVERAGE of the source
2010 BG medians, NOT a true median. The approximation is:

  MHI(2020 BG) = sum( MHI(2010 BG) * Tot_Pop(2010 BG) * weight ) /
                 sum( Tot_Pop(2010 BG) * weight )

where the sums are taken over all 2010 BGs that contribute land
area to the 2020 BG, and weight is the area share of each
contribution. A true median across recombined geographies would
require the underlying household income distribution, which ACS
does not publish at block group level. Use harmonized MHI for
broad comparisons; treat as approximate.

See docs/harmonization.md for the full harmonization methodology.
"""),

    ("Age_Sex_Fields",
     """Population breakdown by sex and 5 age groups.

Source: ACS Table B01001 (Sex by Age), at block group level.

Count fields (male, female, and combined total for each group):
  Male_Under5, Female_Under5, Tot_Under5
  Male_5to17, Female_5to17, Tot_5to17
  Male_18to44, Female_18to44, Tot_18to44
  Male_45to64, Female_45to64, Tot_45to64
  Male_65Plus, Female_65Plus, Tot_65Plus
  Male_Tot, Female_Tot

Percentage fields (percent of total population):
Each count field has a corresponding _Pct suffix, e.g.
Male_Under5_Pct, Female_18to44_Pct, Tot_65Plus_Pct.

B01001 variable mapping:
  Under 5 - Male 003E; Female 027E.
  5-17    - Male 004E+005E+006E; Female 028E+029E+030E.
  18-44   - Male 007E-014E; Female 031E-038E.
  45-64   - Male 015E-019E; Female 039E-043E.
  65+     - Male 020E-025E; Female 044E-049E.
"""),

    ("TotDisab",
     """Total number of persons with a disability in the block group.

Source: ACS Table B18101 (Sex by Age by Disability Status,
civilian noninstitutionalized population), pulled at TRACT level
and joined to block groups. B18101 is not published at block group
geography for Texas; all block groups within a tract share the
same tract-level disability counts and rates.

Calculation:
  TotDisab = DisabUnder18 + Disab18to64 + Disab65Plus

Components:
  DisabUnder18 - Male+female under 5 and 5-17 with a disability
                 (B18101_004E + 007E + 023E + 026E).
  Disab18to64  - Male+female 18-34 and 35-64 with a disability
                 (B18101_010E + 013E + 029E + 032E).
  Disab65Plus  - Male+female 65-74 and 75+ with a disability
                 (B18101_016E + 019E + 035E + 038E).
"""),

    ("DisabPct",
     """Percentage of the civilian noninstitutionalized population
living with a disability.

Source: Tract-level ACS Table B18101, joined to block groups.

Calculation:
  DisabPct = (TotDisab / DisabUniverse) * 100

Note: Because disability data is only available at tract level,
all block groups within the same tract share identical disability rates.
"""),

    ("pop_job_den",
     """Combined population and employment density indicator.

Calculation:
  pop_job_den = Pop_per_acre + Jobs_per_acre
"""),

    ("Transit_Dependency_Index",
     """Composite indicator of likely transit reliance.

Calculation:
  Transit_Dependency_Index =
      (AtBelowPov * 0.30) +
      (MinPopPer  * 0.25) +
      (Transit%   * 0.25) +
      (DisabPct   * 0.20)

Note: DisabPct is sourced at the tract level because B18101 is
not published at block group geography for Texas.
"""),

    ("Transit_Supportive_Density_Index",
     "Transit_Supportive_Density_Index = (Pop_per_acre + Jobs_per_acre) * 10"),

    ("Jobs_Housing_Ratio",
     "Jobs_Housing_Ratio = Jobs / Tot_Pop"),

    ("Jobs_Housing_Balance_Score",
     "Jobs_Housing_Balance_Score = 1 - |1 - Jobs_Housing_Ratio|"),

    ("Transit_Opportunity_Index",
     """Headline composite score combining equity, density, and growth.

Calculation:
  Transit_Opportunity_Index =
      (Transit_Dependency_Index         * 0.4) +
      (Transit_Supportive_Density_Index * 0.3) +
      (Pop_Growth_Pct                   * 0.2) +
      (Jobs_Growth_Pct                  * 0.1)
"""),

    ("Growth_Category",
     "Population Decline (< -5%), Stable (-5% to 0%), Slow Growth (0-5%), Moderate Growth (5-20%), Rapid Growth (> 20%)."),

    ("Combined_Growth_Index",
     "Combined_Growth_Index = (Pop_Growth_Pct * 0.6) + (Jobs_Growth_Pct * 0.4)"),

], columns=["Field", "Definition"])

# -----------------------------
# Run metadata
# -----------------------------
metadata = pd.DataFrame([
    ("Pipeline Run Time",   datetime.now()),
    ("Geographic Level",    "block group"),
    ("ACS Years",           str(acs_years)),
    ("LODES Years",         str(lodes_years)),
    ("Latest ACS Year",     acs_latest),
    ("Latest LODES Year",   lodes_latest),
    ("State FIPS",          state_fips),
    ("Counties",            ",".join(counties_fips)),
    ("CRS",                 "EPSG:3081 Texas Centric Albers Equal Area"),
    ("Age/Sex Source",      "ACS Table B01001 (Sex by Age), block group level"),
    ("Disability Source",   "ACS Table B18101 (Sex by Age by Disability Status), TRACT level joined to BG"),
], columns=["Parameter", "Value"])

# -----------------------------
# Export — GeoPackage
# -----------------------------
gpkg_path = os.path.join(output_folder, "transit_opportunity_bg.gpkg")

if os.path.exists(gpkg_path):
    os.remove(gpkg_path)

final_gdf.to_file(gpkg_path, layer="transit_opportunity_multi_year_bg", driver="GPKG")
current_gdf.to_file(gpkg_path, layer="transit_opportunity_current_bg", driver="GPKG")

# -----------------------------
# Export — CSV
# -----------------------------
final_gdf.drop(columns='geometry').to_csv(
    os.path.join(output_folder, "transit_opportunity_combined_multi_year_bg.csv"),
    index=False
)

current_gdf.drop(columns='geometry').to_csv(
    os.path.join(output_folder, "transit_opportunity_current_year_bg.csv"),
    index=False
)

# -----------------------------
# Export — Excel (with formatted Glossary tab)
# -----------------------------
from openpyxl.styles import Alignment

multi_xlsx = os.path.join(output_folder, "transit_opportunity_combined_multi_year_bg.xlsx")
cur_xlsx   = os.path.join(output_folder, "transit_opportunity_current_year_bg.xlsx")

def format_glossary(ws):

    ws.column_dimensions['A'].width = 28
    ws.column_dimensions['B'].width = 74

    for row in ws.iter_rows(min_row=2):

        ws.row_dimensions[row[0].row].height = 225

        for cell in row:
            cell.alignment = Alignment(horizontal="left", vertical="top", wrap_text=True)

with pd.ExcelWriter(multi_xlsx, engine="openpyxl") as writer:

    final_gdf.drop(columns='geometry').to_excel(writer, "Data",                index=False)
    summary_df.to_excel(writer,                         "Block_Group_Summary", index=False)
    glossary.to_excel(writer,                           "Glossary",            index=False)
    metadata.to_excel(writer,                           "Metadata",            index=False)

    workbook    = writer.book
    glossary_ws = workbook["Glossary"]
    format_glossary(glossary_ws)


with pd.ExcelWriter(cur_xlsx, engine="openpyxl") as writer:

    current_gdf.drop(columns='geometry').to_excel(writer, "Data",                index=False)
    summary_df.to_excel(writer,                           "Block_Group_Summary", index=False)
    glossary.to_excel(writer,                             "Glossary",            index=False)
    metadata.to_excel(writer,                             "Metadata",            index=False)

    workbook    = writer.book
    glossary_ws = workbook["Glossary"]
    format_glossary(glossary_ws)

print("Block group pipeline complete — all outputs created.")
