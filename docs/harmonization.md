# Geographic harmonization: comparing across the 2010–2020 boundary change

This document explains a problem that affects almost every multi-year block-group analysis and gets handled badly (or not at all) by most planning shops, including some that should know better. The problem is simple to state:

> **Block group and tract boundaries changed between the 2010 and 2020 decennial censuses.** A 2019 ACS estimate and a 2024 ACS estimate joined on `BGGEOID` are not actually comparing the same places. Some block groups split in two. Some merged. Some kept the same ID but had their boundary redrawn. A handful are entirely new. The same is true for tracts.

If you don't account for this, your "growth" map is partly real and partly an artifact of redistricting. It's not a small effect — across a fast-growing metro it can affect 10–25% of block groups.

This pipeline handles the problem by *harmonizing* the older-vintage data onto the 2020 geography before computing growth. The harmonization is implemented in [`harmonize_bg.py`](../harmonize_bg.py) and applied to both block-group-level data (the demographic and age-sex tables) and tract-level data (disability), so every join in the pipeline stays vintage-consistent.

---

## Why ACS vintages don't line up

ACS 5-year estimates are tied to the most recent decennial census geography that was in place during the 5-year window:

| ACS 5-year vintage | Geography |
|---|---|
| 2009–2013 through 2016–2020 | 2010 block groups |
| 2017–2021 onward | 2020 block groups |

So an ACS 2019 estimate (the 2015–2019 5-year) is on 2010 BGs. An ACS 2024 estimate (the 2020–2024 5-year) is on 2020 BGs. They are not the same units, even if the GEOIDs sometimes match.

LODES has the same issue across versions, but LODES8 (which this pipeline uses) is internally consistent — it backcasts everything onto 2020 blocks. So the harmonization here is targeted at the ACS side only.

---

## What boundary changes actually look like

Four things happen when block groups are redrawn:

| Change | What it looks like in the data |
|---|---|
| **Identical** | Same boundary, same ID. No problem. |
| **Renumbered** | Same boundary, new ID. Naive join misses it. |
| **Split** | One 2010 BG → multiple 2020 BGs. Naive join attributes the entire 2010 estimate to one of the descendants. |
| **Merged** | Multiple 2010 BGs → one 2020 BG. Naive join picks one and drops the others. |
| **Boundary shift** | Same ID, partly different territory. Naive join silently mixes apples and oranges. |

The Census Bureau publishes a relationship file that documents every one of these cases at the block-group level. That file is the foundation of the harmonization in this repo.

---

## How the harmonization works

The Census [2020 Block Group to 2010 Block Group Relationship File](https://www.census.gov/geographies/reference-files/2020/geo/relationship-files.html) gives one row per (2020 BG, 2010 BG) intersection, with the land area of the intersection and the total land area of each BG.

For each pair, we compute a weight: *what share of the 2010 BG's land area lies within this 2020 BG?*

```
weight = AREALAND_PART / AREALAND_BLKGRP_10
```

Weights for any given 2010 BG sum to ~1.0 across all the 2020 BGs it contributes to.

To apportion a 2010-vintage estimate onto 2020 geography, we multiply each 2010 value by its weight to each 2020 BG, then sum over all 2010 BGs that contribute to that 2020 BG:

```
estimate(2020 BG) = Σ over contributing 2010 BGs of:
                    estimate(2010 BG) × weight(2010 BG → 2020 BG)
```

Concretely, if a 2010 BG with 1,200 people splits into two 2020 BGs that each get half of its land area, each 2020 BG receives 600 people from this source — plus whatever they get from any *other* 2010 BGs that contribute to them.

---

## What can and can't be apportioned

| Variable type | Apportion? | Why |
|---|---|---|
| Counts (population, jobs, households) | **Yes** | Sums to a meaningful total |
| Numerators and denominators of rates | **Yes** | They're counts |
| Percentages (computed rates) | **No** — recompute from harmonized counts | The *ratio* of two harmonized counts is correct; the apportioned ratio is not |
| Median income | **Approximate** — population-weighted average of source medians | A true median has no meaningful sub-area decomposition; the pop-weighted average is a defensible proxy and is flagged in the output |

This is why the pipeline harmonizes count variables and recomputes percentages (`MinPopPer`, `AtBelowPov`, `Transit%`, all the age-sex `_Pct` fields) from the harmonized counts. The percentages are correct.

For MHI, the pipeline computes a population-weighted average of the contributing 2010 BG medians:

```
MHI(2020 BG) = Σ ( MHI(2010 BG) × Tot_Pop(2010 BG) × weight ) /
               Σ ( Tot_Pop(2010 BG) × weight )
```

This is an approximation, **not** a true median. A correct median across recombined geographies would require the underlying household income distribution, which ACS doesn't publish at BG level. The approximation is fine for broad spatial comparisons and trend visualizations, but should not be reported as the true median household income of the new geography. The output Glossary tab flags this.

---

## Limitations

1. **Area weighting, not population weighting.** Where population is unevenly distributed within a 2010 unit, area weighting is a coarse approximation. A more precise approach uses 2010 decennial block populations to weight the apportionment. That's a natural extension of this module and would be the obvious next addition (see *What's next* below).

2. **Differential privacy noise.** The 2020 decennial geography itself was published with privacy-preserving noise injection in some products. For block-group-level analysis the effect is generally small but non-zero.

3. **Edge cases.** A small number of 2020 units (typically water bodies, military installations, or unincorporated territory) have no 2010 antecedent. These pass through with a NaN harmonized estimate, which is the correct behavior.

---

## What's next

- **Population-weighted apportionment.** Pull 2010 decennial block populations from PL 94-171 and aggregate to (2010 unit, 2020 unit) intersection level. Use those as the weight numerator instead of land area. More accurate where density varies within a unit.
- **Apportionment uncertainty.** Each weight has an implied confidence (a 50/50 split is more uncertain than a 99/1). Propagating that into a confidence flag on the growth metric would make the output honest about which growth measurements are clean and which are partly synthetic.

---

## References

- U.S. Census Bureau, [Relationship Files](https://www.census.gov/geographies/reference-files/2020/geo/relationship-files.html). The authoritative source for the BG-to-BG and tract-to-tract files.
- U.S. Census Bureau, [Comparability Relationship File Record Layouts](https://www.census.gov/programs-surveys/geography/technical-documentation/records-layout/2020-comp-record-layout.html). Field definitions for the relationship files used here.
