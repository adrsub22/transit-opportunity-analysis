"""
Geographic harmonization between 2010 and 2020 Census geography.

The problem this module solves
------------------------------
The Census Bureau redrew tract and block-group boundaries between the 2010
and 2020 decennial censuses. ACS 5-year estimates with end years up to and
including 2020 are published on 2010 geography; vintages from 2021 onward
use 2020 geography. A naive join across this break on GEOID will silently
drop split / merged / renumbered geographies and incorrectly attribute
estimates wherever an ID was reused for a redrawn boundary.

The fix is geographic harmonization: apportion estimates from the source
(2010) geography onto the target (2020) geography using the Census Bureau's
official Block Group and Tract Relationship Files. This module implements
both, since BG-level analysis usually rests on tract-level joins (e.g. for
disability data, which Texas only publishes at tract level) and both need
to be on the same vintage to stay consistent.

Method
------
Area-weighted apportionment. For every (2010 geo, 2020 geo) intersection in
the relationship file, the weight is the share of the 2010 unit's land area
that lies within the 2020 unit:

    weight = AREALAND_PART / AREALAND_<unit>_10

For a given 2020 unit, the harmonized estimate is the sum across all 2010
units that contribute to it, each multiplied by its weight. Weights for any
given 2010 unit sum to ~1.0 across all its 2020 partners.

This is appropriate for COUNT variables (population, jobs, households).
RATE variables (percentages, medians) should be recomputed from harmonized
counts rather than apportioned directly.

Population-weighted apportionment is more accurate where population is
unevenly distributed within a unit — see harmonization.md for discussion.

Source
------
U.S. Census Bureau, 2020 Relationship Files
https://www.census.gov/geographies/reference-files/2020/geo/relationship-files.html

Usage
-----
    from harmonize_bg import build_bg_crosswalk, build_tract_crosswalk, harmonize_counts

    bg_cw    = build_bg_crosswalk(state_fips='48')
    tract_cw = build_tract_crosswalk(state_fips='48')

    df_2019_on_2020_bg = harmonize_counts(
        df_2019_bg, bg_cw, count_cols=['Tot_Pop', 'Work_Pop', ...]
    )

    df_disab_on_2020_tract = harmonize_counts(
        df_disab_2019, tract_cw,
        count_cols=['DisabUniverse', 'TotDisab', ...],
        geoid_col='TRACTGEOID',
    )

    # Re-derive any percentages from the harmonized counts.
"""

import os
import requests
import pandas as pd

# State-level relationship files. Pipe-delimited UTF-8 text, one row per
# (2020 unit, 2010 unit) intersection.
_REL_URLS = {
    'blkgrp': (
        "https://www2.census.gov/geo/docs/maps-data/data/rel2020/"
        "blkgrp/tab20_blkgrp20_blkgrp10_st{state}.txt"
    ),
    'tract': (
        "https://www2.census.gov/geo/docs/maps-data/data/rel2020/"
        "tract/tab20_tract20_tract10_st{state}.txt"
    ),
}

# The substring tag the Census uses inside field names for each entity.
_FIELD_TAG = {
    'blkgrp': 'BLKGRP',
    'tract':  'TRACT',
}


# -----------------------------
# Public API
# -----------------------------
def build_bg_crosswalk(state_fips, output_dir='.', force_download=False):
    """
    Build an area-weighted 2010 -> 2020 BLOCK GROUP apportionment crosswalk.

    Returns:
        DataFrame with columns:
            GEOID_2010, GEOID_2020, weight
        Weights for a given GEOID_2010 sum to ~1.0 across its 2020 partners.
    """
    return _build_crosswalk(state_fips, geo='blkgrp', output_dir=output_dir, force_download=force_download)


def build_tract_crosswalk(state_fips, output_dir='.', force_download=False):
    """
    Build an area-weighted 2010 -> 2020 TRACT apportionment crosswalk.

    Same shape and semantics as build_bg_crosswalk(), at tract resolution.
    Useful when joining BG-level data to data only published at tract level
    (e.g. disability), to keep both sides on the same vintage.
    """
    return _build_crosswalk(state_fips, geo='tract', output_dir=output_dir, force_download=force_download)


def harmonize_counts(df, crosswalk, count_cols, geoid_col='BGGEOID'):
    """
    Apportion count variables from 2010 geography onto 2020 geography.

    Use only on COUNT variables (population, jobs, households).
    Rate variables and medians (e.g. percentages, MHI) should NOT be
    apportioned — recompute them from harmonized counts after this step.

    Args:
        df: DataFrame whose rows are 2010-vintage units, with the GEOID
            in `geoid_col` and count variables in `count_cols`.
        crosswalk: Output of build_bg_crosswalk() or build_tract_crosswalk().
        count_cols: List of count column names to apportion.
        geoid_col: Name of the GEOID column in `df`. The same name is used
            on the returned DataFrame, but the values are 2020-vintage.

    Returns:
        DataFrame with one row per 2020 unit and the apportioned count values.
    """
    df = df.copy()
    df[geoid_col] = df[geoid_col].astype(str)

    # Validate inputs early so failures aren't masked by silent column drops.
    missing = [c for c in count_cols if c not in df.columns]
    if missing:
        raise KeyError(
            f"harmonize_counts: input is missing column(s) {missing}. "
            f"Available: {list(df.columns)}"
        )

    # Force numeric dtype on count columns BEFORE the merge so the post-merge
    # multiplication can't accidentally produce object dtype (which would
    # cause the columns to be silently dropped from the groupby aggregation).
    for col in count_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    merged = df.merge(crosswalk, left_on=geoid_col, right_on='GEOID_2010', how='inner')

    if merged.empty:
        return pd.DataFrame(columns=[geoid_col] + count_cols)

    for col in count_cols:
        merged[col] = merged[col] * merged['weight']

    # Explicit column selection in the groupby. Avoids numeric_only=True,
    # which can silently drop columns that look numeric but aren't (e.g.
    # object dtype after an upstream coercion).
    out = (
        merged.groupby('GEOID_2020')[count_cols]
              .sum()
              .reset_index()
              .rename(columns={'GEOID_2020': geoid_col})
    )

    return out


def validate_crosswalk(crosswalk, tol=0.01):
    """
    Sanity-check a crosswalk: weights for each 2010 unit should sum to ~1.0.

    Returns the count of 2010 units whose total weight falls outside
    [1 - tol, 1 + tol]. A small number of edge cases is normal
    (water bodies, extraterritorial areas); a large number suggests
    something is wrong with the relationship file or the parsing.
    """
    sums = crosswalk.groupby('GEOID_2010')['weight'].sum()
    bad  = ((sums < 1 - tol) | (sums > 1 + tol)).sum()
    print(f"Crosswalk check: {bad} of {len(sums)} 2010 units have weights summing outside [{1-tol:.2f}, {1+tol:.2f}]")
    return bad


# -----------------------------
# Internals
# -----------------------------
def _build_crosswalk(state_fips, geo, output_dir='.', force_download=False):

    if geo not in _REL_URLS:
        raise ValueError(f"geo must be one of {list(_REL_URLS)}; got {geo!r}")

    fpath = _download_relationship_file(state_fips, geo, output_dir, force_download)

    rel = pd.read_csv(fpath, sep='|', dtype=str)
    rel.columns = [c.upper() for c in rel.columns]

    tag = _FIELD_TAG[geo]

    # Standard Census field names for these relationship files.
    # If a future release renames them, _find_col raises a clear error.
    geoid_20_col = _find_col(rel, contains=('GEOID', f'{tag}_20'))
    geoid_10_col = _find_col(rel, contains=('GEOID', f'{tag}_10'))
    area_10_col  = _find_col(rel, contains=('AREALAND', f'{tag}_10'))
    part_col     = _find_col(rel, contains=('AREALAND_PART',))

    rel[area_10_col] = pd.to_numeric(rel[area_10_col], errors='coerce')
    rel[part_col]    = pd.to_numeric(rel[part_col],    errors='coerce')

    crosswalk = pd.DataFrame({
        'GEOID_2010': rel[geoid_10_col],
        'GEOID_2020': rel[geoid_20_col],
        'weight':     rel[part_col] / rel[area_10_col].replace(0, pd.NA),
    })

    # Drop rows where the 2010 unit is missing (these are 2020 units that
    # didn't exist in 2010 — no source data to apportion from).
    crosswalk = crosswalk.dropna(subset=['GEOID_2010', 'weight'])

    # GEOID lengths: BG = 12, tract = 11
    expected_len = 12 if geo == 'blkgrp' else 11
    crosswalk = crosswalk[crosswalk['GEOID_2010'].str.len() == expected_len]

    return crosswalk.reset_index(drop=True)


def _download_relationship_file(state_fips, geo, output_dir, force):

    fname = f"tab20_{geo}20_{geo}10_st{state_fips}.txt"
    fpath = os.path.join(output_dir, fname)

    if os.path.exists(fpath) and not force:
        return fpath

    url = _REL_URLS[geo].format(state=state_fips)
    print(f"Downloading {geo} relationship file: {url}")

    r = requests.get(url)
    r.raise_for_status()

    with open(fpath, 'wb') as f:
        f.write(r.content)

    return fpath


def _find_col(df, contains):
    """Find the column whose name contains all the given substrings."""
    matches = [c for c in df.columns if all(s in c for s in contains)]

    if len(matches) == 0:
        raise ValueError(
            f"No column in relationship file contains all of {contains}. "
            f"Available columns: {list(df.columns)}"
        )

    if len(matches) > 1:
        # Prefer the shortest match (least decorated)
        matches.sort(key=len)

    return matches[0]


# -----------------------------
# Quick self-test when run directly
# -----------------------------
if __name__ == '__main__':

    bg_cw = build_bg_crosswalk(state_fips='48')
    print(f"BG crosswalk: {len(bg_cw):,} rows, "
          f"{bg_cw['GEOID_2010'].nunique():,} 2010 BGs, "
          f"{bg_cw['GEOID_2020'].nunique():,} 2020 BGs")
    validate_crosswalk(bg_cw)

    tract_cw = build_tract_crosswalk(state_fips='48')
    print(f"Tract crosswalk: {len(tract_cw):,} rows, "
          f"{tract_cw['GEOID_2010'].nunique():,} 2010 tracts, "
          f"{tract_cw['GEOID_2020'].nunique():,} 2020 tracts")
    validate_crosswalk(tract_cw)
