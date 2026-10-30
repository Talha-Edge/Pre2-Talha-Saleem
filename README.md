# Pre2-Talha-Saleem
### LimeX Capital AI/ML Pre-Internship Program (Batch 2)
**Engineer:** Talha Saleem  

A structured end-to-end data engineering, automated web scraping, and database ingestion pipeline built across the internship milestones.

---

## 📂 Repository Roadmap

| Directory | Task Description | Key Technologies |
| :--- | :--- | :--- |
| **[`Task 1 scrapping 1000`](./Task%201%20scrapping%201000)** | Initial scraping proof-of-concept targeting 1,000 articles across RSS feeds & multi-channel news portals. | Python, Requests, BeautifulSoup, CSV, Threading |
| **[`Task 2 scrapping 1 lack`](./Task%202%20scrapping%201%20lack)** | Production-grade high-throughput distributed scraping targeting 100,000+ national and global news articles with automated summarization and JSON serialization. | Asynchronous Scraping, Parallel Workers, Custom Selectors, Fault-Tolerant Logging |
| **[`Task 3 making database and entering csv data to database`](./Task%203%20making%20database%20and%20entering%20csv%20data%20to%20database)** | Enterprise PostgreSQL database architecture, ACID transactions, modern identity primary keys, dynamic conflict resolution (UPSERT), full CRUD API, and batch ingestion for 70k+ records. | PostgreSQL, psycopg2, execute_values, B-Tree Indexing, dotenv |
| **[`Task 4 data preprocessing pipeline`](./Task%204%20data%20preprocessing%20pipeline)** | Production-grade 8-stage news preprocessing pipeline: text sanitization, HTML stripping, exact & MinHash/LSH near-deduplication, multi-rule validation, quality grading, and Parquet/CSV export. | Pandas, BeautifulSoup4, MinHash/LSH, PyArrow, Parquet, Jupyter |
| **[`Task 5 data validation and quality assessment`](./Task%205%20data%20validation%20and%20quality%20assessment)** | Contract-driven data quality & validation engine: schema conformance, URL/domain checks, temporal audit, length thresholds, duplicate checks, and deterministic triage (ACCEPT, WARNING, QUARANTINE). | Pandas, NumPy, Data Contracts, Pytest, Python |
| **[`Task 6 exploratory data analysis`](./Task%206%20exploratory%20data%20analysis)** | Comprehensive 12-section Exploratory Data Analysis (EDA): schema profiling, missingness, duplicate analysis, text-length distributions, domain concentration, topic/category tagging, UTC temporal trends, and suspicious extraction detection. | Pandas, Matplotlib, NumPy, Jupyter (9.ipynb), Seaborn/Statistical EDA |

---

## 🛠️ Tech Stack & Architecture
- **Data Acquisition:** Python, Selenium, BeautifulSoup4, Requests
- **Database & Storage:** PostgreSQL 16+, SQL, ACID Transactions, Connection Pooling
- **Ingestion & Processing:** Batch chunking (`psycopg2.extras.execute_values`), Pandas, CSV, PyArrow (Parquet)
- **Data Cleaning & NLP:** Regex, BeautifulSoup4, MinHash deduplication, Jaccard similarity, HTML unescape
- **Data Quality & Validation:** Contract specifications, Schema assertion, Gating triage (Accept/Warning/Quarantine), Unit testing
- **Exploratory Data Analysis:** Distribution histograms, boxplots, temporal frequency (UTC), domain composition, keyword taxonomy categorization, heuristic anomaly detection
- **DevOps & Integrity:** Parameterized queries, environment isolation (`python-dotenv`), Git version control

