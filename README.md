# Transit Opportunity Analysis

A reproducible Python pipeline that combines U.S. Census ACS demographics, LEHD LODES employment data, and TIGER/Line geography to build a block-group-level **Transit Opportunity Index** for regional transit planning.

The pipeline is designed to answer a recurring question in transit planning:
> *Which neighborhoods are both most likely to depend on transit and most likely to support it?*

---

## The planning problem

Transit-dependent populations — low-income households, people of color, workers without access to a car, people with disabilities — tend to cluster in some neighborhoods. Transit-supportive land patterns — dense housing, dense employment — tend to cluster in others. Where the two overlap is where transit investment delivers the highest equity-and-ridership return per dollar.

This pipeline takes that question, breaks it into measurable components, and produces a block-group-level dataset and Excel workbook that planners can hand to a client, drop into ArcGIS, or use as input to a service-design model.

---

## What it does (in plain language)

The pipeline pulls four datasets from public APIs, joins them at the block group level, and computes a set of derived measures.

1. **Demographics** — population, race, poverty, transit commute share, and a full age-by-sex breakdown from the ACS 5-year estimates.
2. **Disability** — disability status from ACS Table B18101 at the tract level (the smallest geography it's published at in Texas), joined down to all block groups in each tract.
3. **Employment** — total jobs and jobs by wage band from LEHD LODES, aggregated from census-block to block group.
4. **Geography** — block group polygons from TIGER/Line, projected to Texas Albers (EPSG:3081) for accurate area-based density calculations.

From those, it derives:

- Population and job density per acre
- Population and job change between two vintages
- Three composite indices: **Transit Dependency**, **Transit-Supportive Density**, and **Transit Opportunity**

The full set of variables and definitions is in [`docs/data-dictionary.md`](docs/data-dictionary.md). The reasoning behind the methodological choices is in [`docs/methodology.md`](docs/methodology.md).

---

## What you get

Running the pipeline produces three deliverables in the `outputs/` folder:

| File | Format | Use |
|---|---|---|
| `transit_opportunity_bg.gpkg` | GeoPackage | GIS analysis (ArcGIS, QGIS) |
| `transit_opportunity_combined_multi_year_bg.csv` | CSV | Tabular analysis, joining to other data |
| `transit_opportunity_combined_multi_year_bg.xlsx` | Excel | Client-ready, with **Glossary** and **Metadata** tabs |

The Excel workbook is organized for non-technical readers: a `Data` tab with all variables, a `Block_Group_Summary` tab with the headline metrics, a `Glossary` tab explaining every derived field, and a `Metadata` tab documenting the run.

---

## Running the pipeline

```bash
# Install dependencies
pip install -r requirements.txt

# Set your Census API key (free; sign up at https://api.census.gov/data/key_signup.html)
export CENSUS_API_KEY="your_key_here"

# Run
python pipeline.py
```

The default configuration runs **Bexar County, TX** as the example study area, comparing ACS 2019 to ACS 2024 and LODES 2019 to LODES 2023. The study area, vintages, and output folder are all configurable in the `Configuration` block at the top of `pipeline.py` — point it at any combination of state and county FIPS codes and it runs there.

---

## Repository structure

```
transit-opportunity-analysis/
├── README.md                  ← you are here
├── pipeline.py                ← the full pipeline
├── harmonize_bg.py            ← 2010↔2020 BG and tract harmonization
├── requirements.txt
├── .gitignore
└── docs/
    ├── methodology.md         ← why these choices were made; what the limitations are
    ├── harmonization.md       ← the 2010→2020 boundary-change problem and how it's fixed
    └── data-dictionary.md     ← every field, defined
```

---

## Limitations (the short version)

A few things to know before you trust the numbers. The longer version is in [`docs/methodology.md`](docs/methodology.md).

- **Block group and tract boundaries changed between 2010 and 2020.** ACS vintages with end years ≤ 2020 use 2010 geography; later vintages use 2020 geography. The pipeline handles this by area-weighted apportionment of older vintages onto 2020 geography — see [`docs/harmonization.md`](docs/harmonization.md). Without that step, a "growth" map is partly real and partly an artifact of redistricting.
- **ACS 5-year estimates smooth across 5 years.** The "2024" vintage is really 2020–2024.
- **Disability data is published at tract level in Texas**, not block group. The pipeline applies the tract-level rate uniformly to every block group in the tract. This is documented in the output.
- **LODES injects differential-privacy noise.** Very small block groups can have noisy or undercounted jobs.
- **The index weights are defensible, not definitive.** They reflect one planning framework. The methodology doc walks through the reasoning and how to tune them for a different context.

---

## Usability

Originally built for service-planning use at a public transit agency. Designed to be reusable: change two FIPS codes at the top of the file and it runs anywhere in the U.S.

## About

Built by Andrew Reyna, a transit planner / data analyst based in San Antonio.
Currently working at VIA Metropolitan Transit, with
a background in Urban Planning. Find me at adrsub22@gmail.com
