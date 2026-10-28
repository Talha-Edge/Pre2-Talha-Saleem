# Cleaning Report
_Run at 2026-10-28T09:05:13.393138+00:00_

## Where did every row go?
| Stage | Rows | Note |
|---|---|---|
| 1_ingestion | 39,887 | loaded from 1 file(s) |
| 3_standardization | 39,887 | new columns added, no rows removed |
| 4_validation | 39,887 | 0 marked for rejection, rest only flagged |
| 5_deduplication | 39,887 | 331 rows marked as duplicates (kept 1 per group) |
| 6_cleaning | 39,887 | new columns title_clean, body_clean |
| 7_quality | 39,887 | completeness + quality_score added |
| 8_export_clean | 39,556 | final clean rows |
| 8_export_rejected | 0 | rejected rows (with reason) |
| 8_export_duplicates | 331 | duplicate rows (not kept) |

## Final split
- Clean: **39,556**
- Rejected: **0**
- Duplicates (not kept): **331**
- Clean but flagged for review: **126**
- Overall completeness: **100.0%**

## Validation rule counts
- short_title: 8
- missing_url: 0
- invalid_url: 0
- missing_date: 0
- invalid_date: 0
- missing_title: 0
- empty_body: 0
- short_body: 0
- invalid_url_and_no_body: 0

## Quality distribution
- good: 39,760
- review: 126
- poor: 1

## Lowest-quality sources
| source_std   |   articles |   avg_quality |   pct_poor |
|:-------------|-----------:|--------------:|-----------:|
| bbc          |        113 |      0.924336 |      0     |
| sky_news     |          4 |      0.925    |      0     |
| cnn          |         83 |      0.956627 |      0     |
| fox_news     |       6001 |      0.967055 |      0     |
| dawn         |        438 |      0.969521 |      0     |
| npr          |       5998 |      0.97134  |      0.017 |
| nbc_news     |         22 |      0.979545 |      0     |
| dw           |       2353 |      0.979898 |      0     |
| geo_tv       |        219 |      0.980137 |      0     |
| guardian     |        292 |      0.983562 |      0     |