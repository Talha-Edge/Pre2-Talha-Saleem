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

---

## 🛠️ Tech Stack & Architecture
- **Data Acquisition:** Python, Selenium, BeautifulSoup4, Requests
- **Database & Storage:** PostgreSQL 16+, SQL, ACID Transactions, Connection Pooling
- **Ingestion & Processing:** Batch chunking (`psycopg2.extras.execute_values`), Pandas, CSV
- **DevOps & Integrity:** Parameterized queries, environment isolation (`python-dotenv`), Git version control
