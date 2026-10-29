# Task 5: Data Validation & Quality Engineering Pipeline

A production-grade, contract-driven data validation, anomaly auditing, and disposition engine built strictly according to the **LimeX Scraped News Data Validation & Quality Assessment Lecture Curriculum**.

---

## 📌 Project Overview

Following **Task 1 & Task 2** (large-scale scraping of 100k+ articles), **Task 3** (PostgreSQL database architecture & batch ingestion), and **Task 4** (8-stage preprocessing and sanitization pipeline), **Task 5** establishes rigorous automated validation gates to guarantee data contracts before downstream machine learning, indexing, and LLM fine-tuning.

Preprocessing cleans text, but validation evaluates integrity against strict organizational standards:
- Verifying required schema compliance and absence of duplicate attributes
- Identifying non-standard HTTP/HTTPS URLs and hostnames desynchronized from domain metadata
- Ensuring temporal validity (UTC parse success and ensuring publication time $\le$ scrape time)
- Flagging soft-block errors (Cloudflare challenges, CAPTCHAs, paywalls, access denied)
- Assigning deterministic quality dispositions: **ACCEPT**, **WARNING**, and **QUARANTINE**

---

## 🏗️ 17-Section Architecture & Mechanics

| Section | Topic | Key Engineering Mechanics |
| :---: | :--- | :--- |
| **1** | **Preprocessing vs Validation** | Clarifying boundaries: transformations modify data; validation audits and gates |
| **2** | **Dataset Ingestion** | Automated ingestion of preprocessed `news_clean.csv` with candidate path resolution |
| **3** | **Data Contract Definition** | Explicit declarative specification (`CONTRACT` dictionary) of required/optional columns and thresholds |
| **4** | **Schema Validation** | `validate_schema()` engine auditing column duplicates (`SCHEMA_001`), missing required fields (`SCHEMA_002`), and optional fields (`SCHEMA_003`) |
| **5** | **Validation Helpers** | Standardized issue logger `add_issue()`, whitespace-aware `is_missing()`, robust `is_valid_http_url()` |
| **6** | **Required Fields & Types** | Row-level checks for blank required fields (`FIELD_001`) and non-string text types (`TYPE_001`) |
| **7** | **URL & Domain Validation** | Plausible HTTP(S) syntax checks (`URL_001`), domain vs hostname synchronization checks (`URL_002`) |
| **8** | **Timestamp Auditing** | UTC conversion audit (`TIME_002`), missing scrape traceability (`TIME_001`), temporal causality audit ($pub \le scrape$) (`TIME_003`) |
| **9** | **Content Quality Audits** | Warning checks for minimum title length (`TEXT_001`), minimum body length (`TEXT_002`), and block/CAPTCHA regex detection (`TEXT_003`) |
| **10** | **Duplicate Detection** | Identification of repeated URLs (`DUP_001`) and repeated normalized title/content signatures (`DUP_002`) |
| **11** | **Dataset Quality Metrics** | Issue frequency grouping by severity and rule; distinct affected row counts; column completeness percentages |
| **12** | **Disposition Assignment** | Rule-based triage into **ACCEPT** (clean), **WARNING** (usable with inspection), and **QUARANTINE** (blocked by fatal rules `FIELD_001`, `URL_001`) |
| **13** | **Unit Tests for Rules** | Automated validation self-tests verifying URL parser, missingness detector, and timestamp parsing logic |
| **14** | **Validation Summary** | Export of structured machine-readable JSON summary capturing severity breakdowns and disposition counts |
| **15** | **Artifact Export** | Multi-view CSV serialization (`news_accepted`, `news_warning_review`, `news_quarantine`, `news_validation_issues`) |
| **16** | **Exercises** | Hands-on scenarios: custom rule creation, threshold tuning, and quarantine handling |
| **17** | **Key Takeaways** | Contract stability, idempotency, failure classification, and reproducible auditing |

---

## 📁 Repository Structure

```
Task 5 data validation and quality assessment/
├── 8.ipynb                          # Complete interactive Jupyter notebook (Sections 1 to 17)
├── validate_articles.py             # Standalone production command-line validation script
├── requirements.txt                 # Dependencies (pandas, numpy, pytest, openpyxl)
├── README.md                        # Architectural documentation and run instructions
└── validation_output/               # Generated validation artifacts
    ├── news_validation_all.csv      # Complete dataset augmented with issue counts and dispositions
    ├── news_accepted.csv            # Clean, fully validated records meeting all quality gates
    ├── news_warning_review.csv      # Records with non-fatal warnings (short text, missing authors)
    ├── news_quarantine.csv          # Corrupted or invalid records gated from production
    ├── news_validation_issues.csv   # Detailed granular row-level issue audit log
    ├── news_field_completeness.csv  # Field-by-field completeness percentages
    └── news_validation_summary.json # High-level audit metrics and severity counts
```

---

## 🚀 Execution Guide

### 1. Requirements

Install required dependencies:

```bash
pip install -r requirements.txt
```

### 2. Interactive Notebook (`8.ipynb`)

Open `8.ipynb` in VS Code or Jupyter Notebook to run through all 17 interactive sections:

```bash
jupyter notebook 8.ipynb
```

### 3. Production CLI Script (`validate_articles.py`)

Run the complete batch validation pipeline via command-line:

```bash
# Run validation on the full preprocessed dataset
python validate_articles.py

# Run on a 1,000 row sample
python validate_articles.py --sample 1000

# Specify custom input and output paths
python validate_articles.py --input path/to/news_clean.csv --output-dir custom_validation_output/
```

---

## 📊 Summary Output Sample

```json
{
  "input_rows": 1000,
  "input_columns": 48,
  "total_issues": 530,
  "affected_rows": 528,
  "issues_by_severity": {
    "WARNING": 530
  },
  "issues_by_rule": {
    "TEXT_002": 528,
    "DUP_002": 2
  },
  "dispositions": {
    "accept": 472,
    "warning": 528,
    "quarantine": 0
  }
}
```
