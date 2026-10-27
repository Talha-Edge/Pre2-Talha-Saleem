# Task 3: PostgreSQL Database Pipeline & Batch CSV Ingestion

A production-grade, transactional PostgreSQL data pipeline designed to ingest, deduplicate, and manage large-scale scraped news articles with automated conflict resolution (UPSERT), index optimization, and full CRUD capabilities.

---

## 📌 Project Overview

Following **Task 1** (initial scraping test of 1,000 articles) and **Task 2** (large-scale scraping of 100,000+ news articles), **Task 3** focuses on architecting and establishing the database storage layer:
- **Database Engine:** PostgreSQL 16+
- **Driver:** `psycopg2` with high-performance `execute_values` batch extensions
- **Dataset Capacity:** Scalable to millions of records with sub-millisecond indexed queries
- **Data Integrity:** Strict ACID transactional safety with automatic rollback on error

---

## 🏗️ Architectural Features

### 1. Environment Configuration & Validation
- Fully decoupled database credentials using `python-dotenv`.
- Defensive startup check (`validate_config()`) verifies required `.env` variables (`DB_NAME`, `DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`) before attempting any database operations, failing early with explicit error feedback.

### 2. Modern PostgreSQL Schema Design
- **64-bit Identity Primary Key:** Uses `BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY` (modern SQL standard avoiding legacy serial sequences).
- **Deduplication Constraint:** `url TEXT NOT NULL UNIQUE` guarantees zero redundant records across multiple scraping runs and enables native database-level UPSERTs.
- **Categorization:** `source_type`, `source`, and `channel` fields allow fine-grained categorization across 20+ national and international news outlets.
- **Timezone-Aware Timestamps:** `scraped_at TIMESTAMPTZ` and `updated_at TIMESTAMPTZ` record exact capture and modification times in UTC.
- **Performance Indexes:** Pre-computed B-Tree indexes on `channel`, `source`, `date`, and `scraped_at` ensure high-speed filtering, analytics, and date-range queries.

### 3. Idempotent Upsert Logic (`ON CONFLICT DO UPDATE`)
- Handles re-scrapes seamlessly. If an article URL is already present in the database:
  - Updates title, author, date, headlines, word count, and channel metadata in-place.
  - Automatically advances `updated_at = CURRENT_TIMESTAMP`.
  - Avoids primary key collisions or duplicated database rows.

### 4. High-Throughput Batch Ingestion (`execute_values`)
- Utilizes `psycopg2.extras.execute_values` with parameterized page sizes (2,000 records per batch) instead of slow, individual `INSERT` queries.
- Safely processes 70,000+ records in seconds with low memory overhead.
- Includes automatic fallback resolution for source CSV paths (`articles_bulk_new.csv`, `articles_bulk.csv`).

### 5. Full CRUD API Suite
- **Create:** `save_article(conn, article_data)` — Inserts or updates an article and returns the record ID with insertion status.
- **Read:** `get_article_by_id(conn, article_id)` — Fetches complete article record by Primary Key.
- **Update:** Automated via Upsert using `ON CONFLICT (url) DO UPDATE`.
- **Delete:** `delete_article_by_id(conn, article_id)` — Removes records safely with transaction confirmation.

---

## 📊 Database Schema

```sql
CREATE TABLE IF NOT EXISTS articles (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    source_type VARCHAR(50) NOT NULL DEFAULT 'web',
    source VARCHAR(100) NOT NULL,
    channel VARCHAR(100) NOT NULL,
    title TEXT NOT NULL,
    author VARCHAR(255),
    date VARCHAR(100),
    headlines TEXT,
    words INTEGER DEFAULT 0,
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_articles_channel ON articles(channel);
CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source);
CREATE INDEX IF NOT EXISTS idx_articles_date ON articles(date);
CREATE INDEX IF NOT EXISTS idx_articles_scraped_at ON articles(scraped_at);
```

| Column Name | Data Type | Constraints | Description |
| :--- | :--- | :--- | :--- |
| `id` | `BIGINT` | `IDENTITY PRIMARY KEY` | 64-bit auto-incrementing unique identifier |
| `url` | `TEXT` | `NOT NULL UNIQUE` | Article web address (natural deduplication key) |
| `source_type`| `VARCHAR(50)` | `NOT NULL DEFAULT 'web'`| Format type (web / RSS / API) |
| `source` | `VARCHAR(100)`| `NOT NULL` | Source identifier code (e.g. `geo_news`, `bbc`) |
| `channel` | `VARCHAR(100)`| `NOT NULL` | Clean human-readable channel name |
| `title` | `TEXT` | `NOT NULL` | Article headline / title |
| `author` | `VARCHAR(255)`| `NULL` | Reporter or desk attribution |
| `date` | `VARCHAR(100)`| `NULL` | Publication date string |
| `headlines` | `TEXT` | `NULL` | Sub-headline or article lead paragraph |
| `words` | `INTEGER` | `DEFAULT 0` | Calculated article word count |
| `scraped_at` | `TIMESTAMPTZ` | `DEFAULT CURRENT_TIMESTAMP` | Initial ingestion timestamp |
| `updated_at` | `TIMESTAMPTZ` | `DEFAULT CURRENT_TIMESTAMP` | Last modification / upsert timestamp |

---

## 🚀 Setup & Execution Guide

### 1. Prerequisites
- Python 3.10+
- PostgreSQL server (running locally or remotely)
- Database created (e.g. `CREATE DATABASE news_database;`)

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment Variables
Copy `.env.example` to `.env` and fill in your PostgreSQL credentials:
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=news_database
DB_USER=your_db_user
DB_PASSWORD=your_db_password
INPUT_FILE=C:/path/to/articles_bulk_new.csv
```

### 4. Run the Pipeline
```bash
python app.py
```

### Expected Output:
```text
=================================================================
 POSTGRESQL CRUD & CSV INGESTION PIPELINE
=================================================================
[+] Configuration validated successfully.
[+] Table 'articles' and indexes are ready.

--- 1. CREATE (Sample Record) ---
    -> [Inserted new] article ID: 1 (Geo News)

--- 2. READ (Fetch by Primary Key) ---
    Fetched: ID=1 | Channel=Geo News
    Title  : Pakistan Economic Forum Concludes in Islamabad
    Headlines: Key delegates convened to finalize renewable energy investment deals.

--- 3. UPSERT (Update existing URL without crashing) ---
    -> [Updated existing] article ID: 1 (Geo News)
    Verified updated title: Pakistan Economic Forum Concludes - $5B Deals Signed

--- 4. DELETE (Clean up sample test record) ---
[+] Successfully deleted record ID 1.

--- 5. BULK CSV IMPORT (Ingesting Full Scraped Dataset) ---
[i] Starting high-speed batch ingestion from:
    .../articles_bulk_new.csv
    -> Ingested 2,000 articles into PostgreSQL...
    -> Ingested 4,000 articles into PostgreSQL...
    ...
============================================================
 POSTGRESQL INGESTION COMPLETE
============================================================
  Processed from CSV  : 71,420 articles
  Total in Database   : 71,420 articles in table 'articles'
============================================================

[+] Database connection closed cleanly.
```

---

## 🔒 Security & Best Practices
- **Parameterized SQL Queries:** Prevents SQL injection across all dynamic inputs (`%s` placeholders with `psycopg2`).
- **Credential Protection:** `.env` is excluded via `.gitignore` to prevent sensitive credentials from leaking to source control.
- **Graceful Failure Handling:** Complete transaction rollback (`conn.rollback()`) on any SQL exception prevents hung locks and partial writes.
