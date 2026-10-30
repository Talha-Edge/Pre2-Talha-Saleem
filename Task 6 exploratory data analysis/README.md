# Task 6 — Exploratory Data Analysis (Day 9)

## Overview
EDA on the cleaned news dataset produced by the Day 7 preprocessing pipeline.

## New Columns Added (per Day 9 requirements)
| Column | Description |
|--------|-------------|
| `category` | Keyword-assigned: health / politics / stock / market / energy / technology / other |
| `domain`   | Extracted from `news_url_clean` (already present after Day 7) |

## Files
| File | Purpose |
|------|---------|
| `eda_news.py` | Standalone EDA script — all 12 analysis sections |
| `9.ipynb` | Jupyter notebook — interactive, step-by-step EDA |
| `eda_output/` | All generated charts (PNG) and CSV/JSON summaries |

## How to Run
```bash
# From the Limox root
python "Pre2-Talha-Saleem/Task 6 exploratory data analysis/eda_news.py"

# Custom input
python eda_news.py --input path/to/news_clean.csv --output-dir path/to/eda_output
```

## EDA Results (71,842 rows)
| Metric | Value |
|--------|-------|
| Total rows | 71,842 |
| Total columns | 52 |
| Exact duplicate rows | 0 |
| Duplicate URLs | 0 |
| Duplicate titles | 953 |
| Valid timestamps | 71,842 (100%) |
| Suspicious candidates | 33,616 (46.8%) |
| Pearson r (title vs content) | 0.088 |

## Output Files
- `column_profile.csv` — dtype, missing %, unique values per column  
- `missingness_summary.csv` — missing counts sorted descending  
- `duplicate_summary.csv` — exact / URL / title / content duplicate counts  
- `text_length_stats.csv` — percentile stats for title and content lengths  
- `source_distribution.csv` — article count per domain  
- `category_distribution.csv` — article count per category  
- `eda_summary.json` — machine-readable EDA summary  
- `suspicious_candidates.csv` — rows flagged for manual review  
- `*.png` — charts for all 12 analysis sections

## Pipeline Position
```
Day 4 Scraping → Day 7 Preprocessing → Day 8 Validation → Day 9 EDA (this task)
articles_bulk_new.csv  →  news_clean.csv  →  news_accepted.csv  →  eda_output/
```
