# China Spatial Reallocation Project (2000-2020)

This repository contains a fully reproducible, prefecture-level analysis of how China's population and economic activity reallocated across space between 2000 and 2020, and how those shifts co-evolved with development zones and rail/HSR connectivity.

## Core Question

How did the spatial distribution of population and economic activity in China shift from 2000 to 2020, and how did those shifts co-evolve with development zones and transport connectivity?

## Analytical Framing

- Descriptive, spatial-equilibrium-inspired empirical analysis
- Not a causal identification design
- Focus on co-evolution and trajectory typologies

## What Has Been Built

- End-to-end pipeline script: `scripts/run_china_spatial_reallocation.R`
- Processed panel: `data/processed/analysis_panel_prefecture_year.csv`
- Prefecture typology output: `data/processed/typology_prefecture.csv`
- Metadata:
  - `data/metadata/data_inventory.csv`
  - `data/metadata/variable_dictionary.csv`
- Logs:
  - `logs/pipeline_log.txt`
  - `logs/session_info.txt`
- Visual outputs:
  - Curated orthogonal set in `outputs/tables/table_visual_catalog.csv`
  - Maps in `outputs/maps/`
  - Figures in `outputs/figures/`
- Summary/robustness tables in `outputs/tables/`

## Key Results (Current Run)

From `outputs/tables/table_key_answer_summary.csv`:

- Population concentration change (HHI): **+11.84%**
- Economic concentration proxy change (HHI): **+0.73%**
- HSR-near vs HSR-far population growth gap: **+0.0187 pp/year**
- HSR-near vs HSR-far economic growth gap: **+0.762 pp/year**
- Share of "Mixed Transitional Pattern" prefectures: **58.72%**
- Minimum typology stability vs baseline: **0.8488**
- Minimum ARI vs baseline: **0.6135**

Interpretation: the transformation is multi-regime and spatially heterogeneous, with mixed transitional dynamics dominating, alongside localized corridor-led and zone-rail synergy patterns.

## Data Quality and Robustness

- Structural and missingness checks: `outputs/tables/table_data_quality_checks.csv` (all PASS in current run)
- Typology sensitivity: `outputs/tables/table_typology_stability.csv`
- Corridor robustness: `outputs/tables/table_corridor_robustness.csv`

## How To Reproduce

1. Ensure R (>= 4.5) and required packages are available.
2. From repository root, run:

```bash
Rscript scripts/run_china_spatial_reallocation.R
```

The script rebuilds intermediates/processed outputs and regenerates tables/figures.

## Important Repository Note

- `data/raw/` is excluded from git because raw spatial inputs are very large (several GB and multiple files above GitHub size limits).
- The repository keeps code, metadata, logs, processed data, and outputs required to inspect and present results.
