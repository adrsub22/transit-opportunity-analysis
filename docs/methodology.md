### Methodology

This document walks through the analytical choices in the pipeline. The goal is to be transparent about why each decision was made, what the alternatives were, and where the limitations are. None of these choices are the "right" answer — they're the answer that fit the planning question. They can and should be tuned for a different context.

---

## The planning question

The pipeline was built to answer:

> *Where are the block groups in this region that combine high transit dependency, transit-supportive density, and active growth — and would therefore most reward transit investment?*

This framing matters because it shapes everything else. We're not asking *"where is transit ridership today?"* (a different question, answerable from APC data). We're asking where the **latent opportunity** is — especially for service planning and capital investment that needs to look 5–10 years ahead.

---

## Why block group as the unit of analysis

Block groups are the smallest geography ACS publishes most variables at. They typically contain 600–3,000 people, which is fine-grained enough to distinguish a transit-supportive corridor from the lower-density edge of a suburb, but coarse enough to keep margins of error tolerable.

The alternatives I considered:

- **Tracts** — too coarse for service planning. A single tract often spans both a high-density commercial node and a low-density residential edge.
- **Blocks** — finer, but ACS demographic data isn't published at the block level. The decennial census has block-level race and population, but not income, transit commute, or disability.
- **Hexes or custom grids** — would require disaggregating ACS estimates, which introduces synthetic uncertainty.

Block group is the natural unit when the analysis is demographics-driven.

---

## Data sources and why

| Source | What it provides | Why this source |
|---|---|---|
| ACS 5-year (B01003, B02001, B08301, B19013, C17002, B01001) | Total population, race, transit commute share, median household income, poverty status, age-sex breakdown | Block-group resolution, free, well-documented, comparable across years |
| ACS 5-year (B18101) | Disability status by age and sex | The standard ACS source for disability; tract-level only in Texas |
| LEHD LODES WAC | Jobs by workplace, by wage band | Block-level employment counts, updated annually, unique in being publicly available at this resolution |
| TIGER/Line | Block group polygons | Authoritative Census geography, free, year-matched to ACS |

The pipeline pulls two vintages of each (default ACS 2019 / 2024 and LODES 2019 / 2023) so growth can be measured.

---

## The disability problem (and how the pipeline handles it)

ACS Table B18101 (Sex by Age by Disability Status) is the standard source for disability counts. **In Texas, B18101 is not published at block group level** — only at tract level and above. This is a Census Bureau decision driven by sample-size adequacy.

The pipeline handles this by:

1. Pulling B18101 at the tract level
2. Computing the tract-level disability rate
3. Joining that rate down to every block group in the tract

This is the standard accommodation, but it's worth being explicit about what it means: **all block groups within a single tract receive the same disability percentage**. A planner using this output cannot say "block group A has more people with disabilities than block group B" if A and B are in the same tract — they can only compare across tract boundaries.

The output flags which fields are tract-derived versus block-group-native in the `Glossary` tab of the Excel workbook.

---

## The three composite indices

The pipeline produces three composite indices. Each combines several measured variables with explicit weights. The weights are choices, not facts. Here's the reasoning.

### 1. Transit Dependency Index

```
Transit Dependency = AtBelowPov × 0.30
                   + MinPopPer  × 0.25
                   + Transit%   × 0.25
                   + DisabPct   × 0.20
```

- **Poverty (30%)** is weighted highest because it's the most consistent predictor of transit reliance in U.S. transportation research. Households below the poverty line have substantially lower vehicle access.
- **Race / ethnicity (25%)** is included because of well-documented disparities in vehicle access and historical disinvestment, even after controlling for income.
- **Transit commute share (25%)** is a revealed-preference measure: it captures who already uses transit. It's weighted equal to race so that the dependency definition is informed by actual behavior, not just demographic proxies.
- **Disability (20%)** is weighted lowest *only because* it's tract-derived (see above), so its block-group precision is lower. It's still included because people with disabilities have substantially higher transit reliance and are often invisible in pure income-based equity measures.

Tunable: any agency or client with a different equity framework should adjust these weights. The weights live in one block of `pipeline.py` and changing them is a five-second edit.

### 2. Transit-Supportive Density Index

```
Transit-Supportive Density = (Pop_per_acre + Jobs_per_acre) × 10
```

This is intentionally simple. The transit-and-land-use literature — most prominently Cervero & Kockelman's "3 Ds" (Density, Diversity, Design) and Ewing & Cervero's later "5 Ds" — is consistent that combined population-and-jobs density is the single strongest land-use predictor of transit ridership. The pipeline only captures the first D, but it's the one that does the most work. The ×10 scaling brings the result into roughly the same numerical range as the dependency index for the composite.

A natural extension would be to add Diversity (jobs-housing balance, which the pipeline already computes but doesn't roll into the index) and Design / Distance variables (network connectivity, walk-shed to a stop). See *What I'd add next* below.

### 3. Transit Opportunity Index (the headline)

```
Transit Opportunity = Transit Dependency        × 0.40
                    + Transit-Supportive Density × 0.30
                    + Pop Growth %               × 0.20
                    + Jobs Growth %              × 0.10
```

This is the headline composite. It weights:

- **Dependency (40%)** — equity framing first
- **Density (30%)** — ridership potential
- **Population growth (20%)** — forward-looking; a place that's adding people will need more service
- **Jobs growth (10%)** — also forward-looking, but weighted less because employment demand for transit lags population demand

The weights here reflect a planning philosophy: equity and existing density carry the most weight, with growth as a tilt-the-tie input.

---

## Growth categories

Population growth is binned into five categories:

| Category | Range |
|---|---|
| Population Decline | < −5% |
| Stable | −5% to 0% |
| Slow Growth | 0% to 5% |
| Moderate Growth | 5% to 20% |
| Rapid Growth | > 20% |

The 5% and 20% breakpoints reflect what's typical in U.S. metro areas over a 5-year window. They're approximate but work well as a planning communication tool — they sort block groups into bins a non-technical reader can immediately interpret.

---

## Coordinate system choice

For density calculations, geometry is reprojected to **EPSG:3081 (Texas Centric Albers Equal Area)**. This matters: the unprojected TIGER/Line polygons are in a geographic coordinate system (lat / lon), where `.area` returns square degrees, which is meaningless for a density calculation.

For applications outside Texas, the appropriate Albers projection should be substituted (e.g., EPSG:5070 for CONUS, or a state-specific equal-area projection). The CRS is the only line of code that needs to change.

---

## Limitations to be honest about

1. **ACS 5-year smoothing.** Estimates are an average across 5 years. Year-over-year growth from 2019 to 2024 is really 2015–2019 vs. 2020–2024 — there's overlap, and the 2020–2024 vintage is heavily shaped by pandemic effects.
2. **Margins of error are not propagated.** The pipeline uses ACS point estimates only. For block-group-level reporting on small populations, MOEs can be larger than the estimates themselves. A production version of this pipeline should propagate them through the indices.
3. **LODES suppression.** LODES uses differential-privacy noise injection. Small block groups can have noisy or undercounted jobs.
4. **Disability is tract-level.** See above.
5. **Index weights are not empirically validated.** They're defensible, not derived. A natural next step is to regress index components against observed ridership where the agency has APC/AFC data.

---

## What I'd add next

- **Margin-of-error propagation** through the indices, with a confidence flag on each block group.
- **Validation against observed ridership** where available — the cleanest test of whether the Transit Opportunity Index actually predicts what we say it predicts.
- **A walk-shed variant of density** — replacing the current Euclidean per-acre with population and jobs within a network walk distance of each block group.
- **Comparison to GTFS-derived transit access scores** — letting the index distinguish between block groups that are *transit-opportune but underserved* and *transit-opportune and already well served*.
- **Multi-state parameterization** of the projection so the pipeline runs without code edits in any state.
