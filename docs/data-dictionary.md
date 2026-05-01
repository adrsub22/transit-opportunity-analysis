# Data dictionary

Every field in the output, what it means, and where it comes from. Grouped by category for scanning. The reasoning behind the derived measures lives in [`methodology.md`](methodology.md).

---

## Geography

| Field | Description |
|---|---|
| `BGGEOID` | 12-digit block group GEOID (state + county + tract + block group) |
| `TRACTGEOID` | 11-digit tract GEOID (BGGEOID minus the last digit) — used internally to join tract-level disability data |
| `geometry` | Block group polygon, projected to EPSG:3081 |
| `Area_sqm` | Block group area in square meters |
| `Area_acres` | Block group area in acres (Area_sqm ÷ 4046.856) |

---

## Population and race

| Field | Description | Source |
|---|---|---|
| `Tot_Pop` | Total population | ACS B01003_001E |
| `TotWhtPop` | White-alone population | ACS B02001_002E |
| `TotMinPop` | Non-white population | Tot_Pop − TotWhtPop |
| `MinPopPer` | % non-white | (TotMinPop / Tot_Pop) × 100 |

---

## Income and poverty

| Field | Description | Source |
|---|---|---|
| `MHI` | Median household income. **For pre-2020 vintages on harmonized rows, this is a population-weighted average of source-BG medians, not a true median** — see [`harmonization.md`](harmonization.md). | ACS B19013_001E |
| `PovStatDet` | Population for whom poverty status is determined | ACS C17002_001E |
| `Less50Pov` | Population below 0.5 of the poverty line | ACS C17002_002E |
| `50to99Pov` | Population at 0.5–0.99 of the poverty line | ACS C17002_003E |
| `PovLess100` | Population below the poverty line | Less50Pov + 50to99Pov |
| `AtBelowPov` | % at or below the poverty line | (PovLess100 / PovStatDet) × 100 |

---

## Commute mode

| Field | Description | Source |
|---|---|---|
| `Work_Pop` | Workers 16+ | ACS B08301_001E |
| `Pub_Transit` | Workers commuting by public transit | ACS B08301_010E |
| `Transit%` | % of workers commuting by public transit | (Pub_Transit / Work_Pop) × 100 |

---

## Age and sex (block group level)

Source: ACS Table B01001 (Sex by Age). Five age groups for each sex, plus combined totals and percentages of total population.

| Pattern | Examples |
|---|---|
| `{Sex}_{AgeGroup}` (count) | `Male_Under5`, `Female_18to44`, `Male_65Plus` |
| `Tot_{AgeGroup}` (combined count) | `Tot_Under5`, `Tot_18to44`, `Tot_65Plus` |
| `{Sex}_{AgeGroup}_Pct` (% of total pop) | `Male_18to44_Pct`, `Female_65Plus_Pct` |
| `Tot_{AgeGroup}_Pct` (% of total pop) | `Tot_Under5_Pct`, `Tot_65Plus_Pct` |

Age groups: `Under5`, `5to17`, `18to44`, `45to64`, `65Plus`.

---

## Disability (tract-level, applied to block groups)

> ⚠️ All fields below are computed at tract level (ACS B18101) and applied uniformly to every block group within the tract. Block groups in the same tract will have identical disability values. See [methodology.md → The disability problem](methodology.md#the-disability-problem-and-how-the-pipeline-handles-it).

| Field | Description |
|---|---|
| `DisabUniverse` | Civilian noninstitutionalized population (B18101_001E) |
| `DisabUnder18` | Persons under 18 with a disability |
| `Disab18to64` | Persons 18–64 with a disability |
| `Disab65Plus` | Persons 65+ with a disability |
| `TotDisab` | All persons with a disability |
| `DisabPct` | % of universe with a disability |
| `DisabUnder18Pct` / `Disab18to64Pct` / `Disab65PlusPct` | Same, by age group |

---

## Employment (LODES)

Source: LEHD LODES Workplace Area Characteristics (WAC), aggregated from blocks to block groups.

| Field | Description |
|---|---|
| `Jobs` | Total jobs at workplace (LODES C000) |
| `JobsLess1250` | Jobs paying ≤ $1,250 / month (LODES CE01) |
| `Jobs1251_3333` | Jobs paying $1,251 – $3,333 / month (LODES CE02) |
| `JobsOver3333` | Jobs paying > $3,333 / month (LODES CE03) |

---

## Density

| Field | Formula |
|---|---|
| `Pop_per_acre` | Tot_Pop / Area_acres |
| `Jobs_per_acre` | Jobs / Area_acres |
| `pop_job_den` | Pop_per_acre + Jobs_per_acre |

---

## Growth (between baseline and current vintages)

| Field | Formula |
|---|---|
| `Pop_Growth` | current_pop − baseline_pop |
| `Pop_Growth_Pct` | (Pop_Growth / baseline_pop) × 100 |
| `Jobs_Growth` | current_jobs − baseline_jobs |
| `Jobs_Growth_Pct` | (Jobs_Growth / baseline_jobs) × 100 |
| `Growth_Category` | Decline (<−5%), Stable (−5 to 0), Slow (0–5), Moderate (5–20), Rapid (>20) |
| `Combined_Growth_Index` | (Pop_Growth_Pct × 0.6) + (Jobs_Growth_Pct × 0.4) |

---

## Composite indices

See [methodology.md → The three composite indices](methodology.md#the-three-composite-indices) for reasoning behind the weights.

| Index | Formula |
|---|---|
| `Transit_Dependency_Index` | (AtBelowPov × 0.30) + (MinPopPer × 0.25) + (Transit% × 0.25) + (DisabPct × 0.20) |
| `Transit_Supportive_Density_Index` | (Pop_per_acre + Jobs_per_acre) × 10 |
| `Jobs_Housing_Ratio` | Jobs / Tot_Pop |
| `Jobs_Housing_Balance_Score` | 1 − \|1 − Jobs_Housing_Ratio\| |
| `Transit_Opportunity_Index` | (Transit_Dependency × 0.40) + (Supportive_Density × 0.30) + (Pop_Growth_Pct × 0.20) + (Jobs_Growth_Pct × 0.10) |

---

## Run metadata

| Field | Description |
|---|---|
| `dataset_type` | `baseline_comparison` or `current_snapshot` |
| `acs_year` | ACS vintage for the row |
| `lodes_year` | LODES vintage for the row |
| `acs_date` / `lodes_date` | Same as above, as a datetime |
