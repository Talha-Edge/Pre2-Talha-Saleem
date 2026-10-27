"""
================================================================================
PostgreSQL Management & Upsert Pipeline
================================================================================
Features:
  1. Environment Configuration & Validation (checks .env before running)
  2. Safe Connection & Transaction Handling (try / commit / rollback)
  3. Modern Table Definition (BIGINT IDENTITY, TIMESTAMPTZ, UNIQUE constraints)
  4. Upsert Logic (ON CONFLICT ... DO UPDATE)
  5. Full CRUD operations (Create, Read, Update, Delete)
================================================================================
"""

import os
import sys
import csv
from pathlib import Path
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load key-value pairs from your local .env file
load_dotenv()


# ==============================================================================
# SECTION 1: CONFIGURATION & VALIDATION
# ==============================================================================

def validate_config() -> dict:
    """
    Validates that all required database credentials exist in the environment.
    Stops the script immediately with a clear error if anything is missing.
    """
    required_keys = ["DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "DB_PORT"]
    missing = [key for key in required_keys if not os.getenv(key)]

    if missing:
        print(f"[!] Configuration Error: Missing required .env keys: {', '.join(missing)}")
        print("[!] Please verify your .env file before running.")
        sys.exit(1)

    config = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST", "localhost"),
        "port": os.getenv("DB_PORT", "5432"),
    }
    print("[+] Configuration validated successfully.")
    return config


# ==============================================================================
# SECTION 2: CONNECTION & SESSION MANAGEMENT
# ==============================================================================

def get_connection(config: dict):
    """
    Establishes and returns an authenticated connection to PostgreSQL.
    """
    try:
        conn = psycopg2.connect(**config)
        return conn
    except psycopg2.OperationalError as e:
        print(f"[!] Database connection failed: {e}")
        sys.exit(1)


# ==============================================================================
# SECTION 3: SCHEMA CREATION
# ==============================================================================

CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS articles (
    -- Modern 64-bit auto-incrementing primary key
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Unique URL ensures we never store the exact same link twice and enables UPSERT
    url TEXT NOT NULL UNIQUE,

    -- Source & Channel categorization
    source_type VARCHAR(50) NOT NULL DEFAULT 'web',
    source VARCHAR(100) NOT NULL,
    channel VARCHAR(100) NOT NULL,

    -- Article metadata & headline
    title TEXT NOT NULL,
    author VARCHAR(255),
    date VARCHAR(100),
    headlines TEXT,
    words INTEGER DEFAULT 0,

    -- Timezone-aware timestamps
    scraped_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Fast B-Tree indexes for efficient filtering, analytics, and deduplication
CREATE INDEX IF NOT EXISTS idx_articles_channel ON articles(channel);
CREATE INDEX IF NOT EXISTS idx_articles_source ON articles(source);
CREATE INDEX IF NOT EXISTS idx_articles_date ON articles(date);
CREATE INDEX IF NOT EXISTS idx_articles_scraped_at ON articles(scraped_at);
"""

def create_table(conn) -> None:
    """Creates the target table and its indexes if they do not exist."""
    try:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_TABLE_QUERY)
        conn.commit()
        print("[+] Table 'articles' and indexes are ready.")
    except Exception as e:
        conn.rollback()
        print(f"[!] Failed to create table: {e}")
        raise


# ==============================================================================
# SECTION 4: CRUD & UPSERT FUNCTIONS
# ==============================================================================

UPSERT_ARTICLE_QUERY = """
INSERT INTO articles (
    url,
    source_type,
    source,
    channel,
    title,
    author,
    date,
    headlines,
    words
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (url)
DO UPDATE SET
    source_type = EXCLUDED.source_type,
    source = EXCLUDED.source,
    channel = EXCLUDED.channel,
    title = EXCLUDED.title,
    author = EXCLUDED.author,
    date = EXCLUDED.date,
    headlines = EXCLUDED.headlines,
    words = EXCLUDED.words,
    updated_at = CURRENT_TIMESTAMP
RETURNING id, (xmax = 0) AS was_inserted;
"""

def save_article(conn, article_data: dict) -> int:
    """
    Inserts a new article, or updates it in-place if the URL already exists.
    Returns the article's Primary Key (id).
    """
    values = (
        article_data["url"],
        article_data.get("source_type", "web"),
        article_data.get("source", "unknown"),
        article_data.get("channel", "Unknown Channel"),
        article_data["title"],
        article_data.get("author", "Unknown"),
        article_data.get("date", ""),
        article_data.get("headlines", ""),
        int(article_data.get("words", 0)),
    )

    try:
        with conn.cursor() as cursor:
            cursor.execute(UPSERT_ARTICLE_QUERY, values)
            record_id, was_inserted = cursor.fetchone()
        conn.commit()

        action = "Inserted new" if was_inserted else "Updated existing"
        print(f"    -> [{action}] article ID: {record_id} ({article_data.get('channel')})")
        return record_id

    except Exception as e:
        conn.rollback()
        print(f"[!] Error saving article '{article_data.get('url')}': {e}")
        raise


def get_article_by_id(conn, article_id: int) -> dict:
    """Reads a single article by its Primary Key ID."""
    query = """
    SELECT id, url, source_type, source, channel, title, author, date, headlines, words, scraped_at, updated_at
    FROM articles
    WHERE id = %s;
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (article_id,))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "url": row[1],
                "source_type": row[2],
                "source": row[3],
                "channel": row[4],
                "title": row[5],
                "author": row[6],
                "date": row[7],
                "headlines": row[8],
                "words": row[9],
                "scraped_at": row[10],
                "updated_at": row[11],
            }
    except Exception as e:
        conn.rollback()
        print(f"[!] Error reading article: {e}")
        raise


def delete_article_by_id(conn, article_id: int) -> bool:
    """Deletes an article by its Primary Key ID."""
    query = "DELETE FROM articles WHERE id = %s RETURNING id;"
    try:
        with conn.cursor() as cursor:
            cursor.execute(query, (article_id,))
            deleted = cursor.fetchone()
        conn.commit()
        return deleted is not None
    except Exception as e:
        conn.rollback()
        print(f"[!] Error deleting article: {e}")
        raise


# ==============================================================================
# SECTION 5: BULK CSV INGESTION
# ==============================================================================

def import_csv_to_db(conn, csv_path: str = None, batch_size: int = 2000) -> int:
    """
    Imports all scraped articles from CSV into PostgreSQL in fast batches
    using execute_values with ON CONFLICT (url) DO UPDATE.
    """
    if not csv_path:
        csv_path = os.getenv("INPUT_FILE")

    candidates = [
        Path(csv_path) if csv_path else None,
        Path(__file__).resolve().parent / "articles_bulk_new.csv",
        Path(__file__).resolve().parent.parent / "day 4" / "output_bulk" / "final" / "articles_bulk_new.csv",
        Path(__file__).resolve().parent.parent / "day 4" / "output_bulk" / "final" / "articles_bulk.csv",
    ]

    csv_file = next((p for p in candidates if p and p.exists()), None)
    if not csv_file:
        print(f"[!] Error: Could not find CSV file at: {csv_path}")
        return 0

    print(f"\n[i] Starting high-speed batch ingestion from:\n    {csv_file}")
    csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))

    upsert_sql = """
    INSERT INTO articles (
        url,
        source_type,
        source,
        channel,
        title,
        author,
        date,
        headlines,
        words
    )
    VALUES %s
    ON CONFLICT (url)
    DO UPDATE SET
        source_type = EXCLUDED.source_type,
        source = EXCLUDED.source,
        channel = EXCLUDED.channel,
        title = EXCLUDED.title,
        author = EXCLUDED.author,
        date = EXCLUDED.date,
        headlines = EXCLUDED.headlines,
        words = EXCLUDED.words,
        updated_at = CURRENT_TIMESTAMP;
    """

    total_rows = 0
    batch = []

    with open(csv_file, "r", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get("url", "").strip()
            if not url:
                continue

            try:
                words = int(row.get("words", 0) or 0)
            except ValueError:
                words = 0

            record = (
                url,
                row.get("source_type", "web") or "web",
                row.get("source", "unknown") or "unknown",
                row.get("channel", "Unknown Channel") or "Unknown Channel",
                row.get("title", "") or "No Title",
                row.get("author", "") or "Unknown",
                row.get("date", "") or "",
                row.get("headlines", "") or "",
                words,
            )
            batch.append(record)

            if len(batch) >= batch_size:
                with conn.cursor() as cursor:
                    execute_values(cursor, upsert_sql, batch, page_size=batch_size)
                conn.commit()
                total_rows += len(batch)
                print(f"    -> Ingested {total_rows:,} articles into PostgreSQL...")
                batch = []

        if batch:
            with conn.cursor() as cursor:
                execute_values(cursor, upsert_sql, batch, page_size=batch_size)
            conn.commit()
            total_rows += len(batch)
            print(f"    -> Ingested {total_rows:,} articles into PostgreSQL...")

    with conn.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM articles;")
        db_count = cursor.fetchone()[0]

    print("\n" + "=" * 60)
    print(" POSTGRESQL INGESTION COMPLETE")
    print("=" * 60)
    print(f"  Processed from CSV  : {total_rows:,} articles")
    print(f"  Total in Database   : {db_count:,} articles in table 'articles'")
    print("=" * 60 + "\n")
    return total_rows


# ==============================================================================
# SECTION 6: DEMONSTRATION & VERIFICATION FLOW
# ==============================================================================

if __name__ == "__main__":
    print("=" * 65)
    print(" POSTGRESQL CRUD & CSV INGESTION PIPELINE")
    print("=" * 65)

    # 1. Validate environment configuration
    config = validate_config()

    # 2. Connect to database
    conn = get_connection(config)

    try:
        # 3. Create table with Primary Key & Constraints
        create_table(conn)

        # 4. Demonstrate CREATE (First Insert)
        print("\n--- 1. CREATE (Sample Record) ---")
        sample_article = {
            "url": "https://www.geo.tv/latest/10001",
            "source_type": "web",
            "source": "geo_news",
            "channel": "Geo News",
            "title": "Pakistan Economic Forum Concludes in Islamabad",
            "author": "Web Desk",
            "date": "2026-10-27",
            "headlines": "Key delegates convened to finalize renewable energy investment deals.",
            "words": 10,
        }
        art_id = save_article(conn, sample_article)

        # 5. Demonstrate READ
        print("\n--- 2. READ (Fetch by Primary Key) ---")
        fetched = get_article_by_id(conn, art_id)
        print(f"    Fetched: ID={fetched['id']} | Channel={fetched['channel']}")
        print(f"    Title  : {fetched['title']}")
        print(f"    Headlines: {fetched['headlines']}")

        # 6. Demonstrate UPSERT (Update on conflict using same URL)
        print("\n--- 3. UPSERT (Update existing URL without crashing) ---")
        updated_data = {
            "url": "https://www.geo.tv/latest/10001",  # Same URL
            "source_type": "web",
            "source": "geo_news",
            "channel": "Geo News",
            "title": "Pakistan Economic Forum Concludes - $5B Deals Signed",
            "author": "Chief Reporter",
            "date": "2026-10-27",
            "headlines": "Updated: Key delegates finalized $5B in energy deals.",
            "words": 9,
        }
        same_id = save_article(conn, updated_data)

        re_fetched = get_article_by_id(conn, same_id)
        print(f"    Verified updated title: {re_fetched['title']}")

        # 7. Demonstrate DELETE (Clean up test record)
        print("\n--- 4. DELETE (Clean up sample test record) ---")
        delete_article_by_id(conn, art_id)

        # 8. Ingest the 71k+ scraped CSV dataset into PostgreSQL
        print("\n--- 5. BULK CSV IMPORT (Ingesting Full Scraped Dataset) ---")
        import_csv_to_db(conn)

    finally:
        # Always close connection when finished
        conn.close()
        print("\n[+] Database connection closed cleanly.")