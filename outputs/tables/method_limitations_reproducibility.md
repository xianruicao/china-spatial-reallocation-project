# Method Limitations and Reproducibility Notes

## Scope and design
- This project is descriptive and association-focused; it does not establish causal treatment effects.
- Core inference triangulates three descriptive-evidence layers: distributional summaries, bootstrap/permutation gap tests, and adjusted regression models.

## What Was Tightened in This Version
- Rail and HSR geometry uses annual NBER CSTD snapshots rather than back-cast current OSM lines.
- Added uncertainty-aware HSR proximity inference: bootstrap confidence intervals + permutation p-values.
- Added threshold-sensitivity diagnostics for near/far HSR definitions (5-40 km near cutoff; far >50 km).
- Added adjusted driver models (population model with province FE; economic model with provincial-growth controls) and bootstrap coefficient intervals.
- Expanded visual evidence with integrated hotspot map and robustness-focused figures.

## Key data limitations
- Development zones are compiled from Wikidata points and may be incomplete for sub-provincial inventories.
- Prefecture GDP is still a population-allocated provincial proxy, not official prefecture GDP series.
- Provincial GDP values are nominal snapshots scraped from Wikipedia and interpolated at missing snapshot years.
- Population uses GPW snapshots (2000, 2005, 2010, 2015, 2020), not annual census-equivalent estimates.

## Reproducibility
- Main pipeline script: `scripts/run_china_spatial_reallocation.R`
- Run command: `Rscript scripts/run_china_spatial_reallocation.R`
- Runtime/session metadata: `logs/pipeline_log.txt`, `logs/session_info.txt`
- Core outputs: `data/processed/analysis_panel_prefecture_year.csv`, `data/processed/typology_prefecture.csv`
- Robustness outputs: `outputs/tables/table_hsr_gap_inference.csv`, `outputs/tables/table_hsr_threshold_sensitivity.csv`, `outputs/tables/table_driver_regression_bootstrap.csv`
