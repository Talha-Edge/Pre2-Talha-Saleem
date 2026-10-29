# Task 4: News Article Data Preprocessing & Quality Engineering

A modular, production-grade data preprocessing, sanitization, and quality assurance pipeline built strictly according to the **LimeX Scraped News Data Preprocessing Lecture Curriculum**.

---

## 📌 Project Overview

Following **Task 1 & Task 2** (large-scale scraping of 100k+ news articles) and **Task 3** (PostgreSQL transactional storage and batch upsert ingestion), **Task 4** focuses on raw data engineering, text sanitization, and machine-learning-ready feature extraction.

Scraped web articles often contain noise: raw HTML tags, embedded scripts, broken character references, tracking query parameters, timezone mismatches, duplicate syndicated wire reports, malformed URLs, and non-article error pages. This pipeline systematically audits, cleans, validates, and partitions raw scraped articles into production datasets.

---

## 🏗️ 18-Section Lecture Pipeline Architecture

The pipeline follows the exact curriculum sequence established in the LimeX Pre-Internship Data Preprocessing lecture:

| Section | Topic | Key Engineering Mechanics |
| :---: | :--- | :--- |
| **0** | **Concepts Before Coding** | Data pipelines as transformation graphs; raw vs clean separation; auditability |
| **1** | **Setup and Imports** | `pandas`, `numpy`, `unicodedata`, `re`, `html`, `urllib.parse`, `matplotlib` |
| **2** | **Dataset Ingestion** | Dynamic file resolution (`articles_bulk_new.csv`, fallback detection, sampling toggle) |
| **3** | **Exploratory Profiling** | Non-destructive profiling: dtypes, null counts, null percentages, distinct values |
| **4** | **Schema Standardization** | Standardizing column names via NFKC regex, canonical coalescing into `news_url`, `title`, `published_time`, `author`, `content` |
| **5** | **Missing & Whitespace** | Preservation of `_raw` columns; mapping tokens (`n/a`, `null`, `none`, `-`) to `pd.NA`; whitespace & newline normalization |
| **6** | **Text & HTML Cleaning** | `HTMLParser` markup stripping (ignoring script/style/noscript), entity unescaping, typography normalization, invisible character removal |
| **7** | **URL Validation** | Protocol verification (`http://`/`https://`), netloc extraction, domain parsing, optional tracking parameter strip |
| **8** | **Publication Timestamps** | Safe parsing to UTC with `format="mixed"`; decomposition into `published_date_utc`, `year`, `month`, `weekday` |
| **9** | **Text Feature Engineering** | Character & word counts for titles and content; detection of error/paywall patterns; length threshold flags |
| **10** | **Duplicate Detection** | Multi-tier duplicate flagging: 1) exact canonical match, 2) duplicate URL, 3) normalized title + body signature |
| **11** | **Row-Level Auditing** | Tiered classification: `critical_issues`, `review_issues`, `info_issues`, `is_critical`, `needs_review` |
| **12** | **Dataset-Level Metrics** | Per-field completeness report table & structured machine-readable JSON metrics summary |
| **13** | **Dataset Partitioning** | Sorting & splitting into `news_clean`, `news_review`, `news_rejected` with strict row reconciliation assertions |
| **14** | **Reproducible Exports** | CSV exports with `utf-8-sig` encoding, excluding temporary internal signatures; JSON audit summary |
| **15** | **Quality Visualizations** | Completeness bar charts and article content length distribution histograms via `matplotlib` |
| **16** | **Source-Level Quality** | Groupby domain aggregation: missing content rates, short content rates, and median article lengths |
| **17** | **Near-Duplicate TF-IDF** | TF-IDF character/word n-gram vectorization with cosine similarity matrix for near-duplicate syndication detection |
| **18** | **Production Checklist** | Idempotency, schema stability, zero silent row loss, and audit trail compliance |

---

## 📁 Repository Structure

```
Task 4 data preprocessing pipeline/
├── 7.ipynb                          # Complete interactive Jupyter notebook (Sections 0 to 18)
├── clean_articles.py                # Standalone automated command-line execution script
├── requirements.txt                 # Dependencies (pandas, numpy, openpyxl, matplotlib, scikit-learn)
├── README.md                        # Architectural documentation and run instructions
└── preprocessed_output/             # Pipeline output artifacts
    ├── news_clean.csv               # High-quality clean dataset ready for NLP/ML
    ├── news_review.csv              # Rows needing manual or rule-based review
    ├── news_rejected.csv            # Invalid records (missing URLs, titles, bodies, exact dups)
    ├── news_quality_report.csv      # Completeness statistics across all canonical attributes
    └── news_preprocessing_summary.json # High-level counts and validation metrics
```

---

## 🚀 Execution Guide

### 1. Requirements

Install required dependencies:

```bash
pip install -r requirements.txt
```

### 2. Interactive Notebook (`7.ipynb`)

Open `7.ipynb` in VS Code or Jupyter Notebook. It contains the complete lecture walkthrough with all explanations, code blocks, visual outputs, and exercises:

```bash
jupyter notebook 7.ipynb
```

> **Tip:** You can set `SAMPLE_ROWS = 1000` in Cell 5 for rapid interactive testing or `SAMPLE_ROWS = None` to process the entire dataset.

### 3. Production CLI Script (`clean_articles.py`)

Run the complete 13-step batch pipeline via command-line:

```bash
# Run on full dataset
python clean_articles.py

# Run on a 1,000 row sample
python clean_articles.py --sample 1000

# Specify custom input and output directory
python clean_articles.py --input path/to/articles.csv --output-dir custom_output/
```

---

## 📊 Summary Output Sample

```json
{
  "rows": 1000,
  "columns": 10,
  "invalid_or_missing_urls": 0,
  "missing_titles": 0,
  "missing_publication_times": 0,
  "missing_authors": 482,
  "missing_content": 0,
  "critical_rows": 0,
  "rows_needing_review": 956,
  "exact_duplicate_rows": 0,
  "duplicate_url_rows": 0,
  "duplicate_text_rows": 0,
  "clean_rows": 1000,
  "review_rows": 956,
  "rejected_rows": 0
}
```
