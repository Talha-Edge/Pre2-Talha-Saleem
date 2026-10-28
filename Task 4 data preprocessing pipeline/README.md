# Task 4: News Article Data Preprocessing & Cleaning Pipeline

A production-grade, modular 8-stage data preprocessing and quality assurance pipeline designed to clean, standardize, validate, deduplicate, and score large-scale scraped news articles.

---

## 📌 Project Overview

Following **Task 1 & Task 2** (scraping 100k+ news articles) and **Task 3** (database storage and batch ingestion), **Task 4** focuses on raw data engineering, text sanitization, and machine-learning-ready feature extraction.

Scraped web articles often contain noise: raw HTML snippets, navigational boilerplate ("Click here to subscribe"), missing timestamps, duplicate syndicated wire reports, malformed URLs, and non-article content. This pipeline systematically filters out noisy records, normalizes text into canonical format, and produces clean CSV and Parquet outputs accompanied by automated audit reports.

---

## 🏗️ 8-Stage Architecture

```
Raw CSV Ingestion
       │
       ▼
Stage 1: Ingestion & Column Normalization
       │
       ▼
Stage 2: Exploratory Profiling & Missingness Auditing
       │
       ▼
Stage 3: Schema Standardization & Canonical Datatypes
       │
       ▼
Stage 4: Validation Engine & Rule-Based Filtering
       │
       ▼
Stage 5: Deduplication (Exact URL/Content + Near-Duplicate LSH)
       │
       ▼
Stage 6: Text Cleaning, HTML Sanitization & Boilerplate Stripping
       │
       ▼
Stage 7: Multi-Metric Quality Assessment & Grading (A/B/C/F)
       │
       ▼
Stage 8: Partitioned Export (CSV & Parquet) + Audit Reporting
```

### Detailed Breakdown of Stages

| Stage | Name | Description | Key Mechanics |
| :---: | :--- | :--- | :--- |
| **1** | **Ingestion** | Dynamic file resolution with fallback search across known directories | Fast chunk-safe ingestion with pandas, UTF-8 normalization |
| **2** | **Profiling** | Deep initial inspection of raw attributes | Field nullity percentages, character/word length percentiles (min, median, max) |
| **3** | **Standardization** | Schema unification into standard naming convention | Standardizes schema into `url`, `title`, `author`, `date`, `full_text`, `headlines`, `channel`, `source` |
| **4** | **Validation** | Hard gate filters removing corrupted or incomplete items | URL regex validation, minimum length (min 15 words), non-empty title/text |
| **5** | **Deduplication** | Multi-level duplicate elimination | 1) Exact URL & body SHA-256 hashing<br>2) Near-duplicate detection via MinHash/Jaccard similarity |
| **6** | **Text Cleaning** | Deep HTML entity decoding and noise removal | HTML tag stripping via BeautifulSoup, unicode unescaping, boilerplate regex removal, whitespace normalization |
| **7** | **Quality Scoring** | Continuous multi-field completeness & quality grade assignment | Weighted completeness score (0–100%) and categorizing into Grades A, B, C, F |
| **8** | **Export & Reports** | Serialization to disk in dual formats with verification | High-compression Parquet + CSV exports, comprehensive JSON & Markdown audit reports |

---

## 📁 Repository Structure

```
Task 4 data preprocessing pipeline/
├── clean_articles.py       # Production pipeline script (all 8 stages)
├── 7.ipynb                 # Interactive Jupyter notebook walkthrough with stage demos
├── requirements.txt        # Python library dependencies
├── README.md               # Pipeline architecture & documentation
└── reports/
    ├── cleaning_report.json # Machine-readable run audit metrics & statistics
    └── cleaning_report.md   # Human-readable summary table of cleaning run
```

---

## 🚀 Getting Started

### 1. Installation

Install required dependencies:

```bash
pip install -r requirements.txt
```

### 2. Running the Interactive Notebook

For an interactive step-by-step walkthrough of each stage with intermediate visual outputs:

```bash
jupyter notebook 7.ipynb
# or open directly in VS Code
```

> **Learning Tip:** The notebook has a `SAMPLE = 500` toggle in Cell 2 for rapid iteration and testing (~30 seconds). Set `SAMPLE = None` to process the entire dataset.

### 3. Running the Production Pipeline Script

To run the complete automated batch cleaning process:

```bash
python clean_articles.py
```

Optional arguments:
- `--sample 1000`: Run on a subset of 1,000 records
- `--input path/to/file.csv`: Specify a custom input CSV path
- `--output-dir data/clean/`: Specify custom output directory

---

## 📊 Pipeline Results & Quality Summary

Processing summary across ~40,000 raw article records:

| Metric | Result |
| :--- | :--- |
| **Input Rows Processed** | 39,598 |
| **Valid Clean Rows Output** | 39,556 (99.89%) |
| **Rejected / Low Quality Rows** | 42 (0.11%) |
| **Exact & Near Duplicates Removed** | 12 |
| **HTML Entities Decoded & Sanitized** | 100% of body text |
| **Final Quality Grade A & B Articles** | > 99.5% |
| **Export Formats** | CSV + Optimized Parquet (Snappy) |

---

## 🛠️ Tech Stack
- **Data Manipulation:** `pandas`, `numpy`
- **Text Processing & Sanitization:** `beautifulsoup4`, `lxml`, `re`, `html`
- **Deduplication:** MinHash, Jaccard Similarity, MD5/SHA-256
- **Storage & Serialization:** `pyarrow` (Parquet), CSV
