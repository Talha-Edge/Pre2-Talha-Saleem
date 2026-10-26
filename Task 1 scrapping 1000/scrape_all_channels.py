"""
Experiment 1: Master Hybrid Scraper (RSS + HTML Parsing)
=======================================================
Objective:
- Reads channels from Day1/feeds.json (10 verified news channels)
- Takes 10 latest articles from each channel (100 articles total)
- Visits each article URL to scrape full text and author
- Streams records row-by-row into Day1/news_articles.csv using csv.DictWriter
- Respectful scraping with polite request delays and clean error handling
"""

import csv
import json
import os
import sys
import time
import requests
from bs4 import BeautifulSoup

# Ensure Windows terminal doesn't crash on Unicode characters
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Standard browser User-Agent to avoid simple bot blocks
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

CSV_FILE = "Day1/news_articles.csv"
FEEDS_CONFIG = "Day1/feeds.json"
ARTICLES_PER_CHANNEL = 100
REQUEST_DELAY_SECONDS = 0.5

# Define the exact columns requested
CSV_COLUMNS = [
    "channel_name",
    "title",
    "url",
    "time",
    "author",
    "summary",
    "content"
]


def load_feeds(config_path):
    """Reads the JSON config containing news channels and their RSS URLs."""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def fetch_channel_articles(channel_name, feed_url, max_articles=100):
    """
    Downloads the RSS XML feed and extracts basic metadata for up to max_articles:
    - title
    - link (URL)
    - pubDate (time)
    - summary (description)
    """
    articles_meta = []
    try:
        response = requests.get(feed_url, headers=HEADERS, timeout=10)
        if response.status_code != 200:
            print(f"  [!] HTTP {response.status_code} when fetching feed for {channel_name}")
            return articles_meta

        soup = BeautifulSoup(response.content, "xml")
        items = soup.find_all("item")
        if not items:
            items = soup.find_all("entry")  # fallback for Atom feeds

        for item in items[:max_articles]:
            # Title
            title_tag = item.find("title")
            title = title_tag.get_text(strip=True) if title_tag else "N/A"

            # URL Link
            link_tag = item.find("link")
            if link_tag:
                link = link_tag.get("href") or link_tag.get_text(strip=True)
            else:
                link = "N/A"

            # Published Date / Time
            date_tag = item.find("pubDate") or item.find("published") or item.find("updated") or item.find("dc:date")
            pub_date = date_tag.get_text(strip=True) if date_tag else "N/A"

            # Summary / Description
            desc_tag = item.find("description") or item.find("summary")
            summary = desc_tag.get_text(strip=True) if desc_tag else "N/A"
            # Strip any residual HTML tags inside summary if present
            if "<" in summary and ">" in summary:
                summary = BeautifulSoup(summary, "html.parser").get_text(strip=True)

            # Author (from feed if available)
            creator_tag = item.find("dc:creator") or item.find("author")
            author = creator_tag.get_text(strip=True) if creator_tag else ""

            articles_meta.append({
                "channel_name": channel_name,
                "title": title,
                "url": link,
                "time": pub_date,
                "author": author,
                "summary": summary
            })

    except Exception as e:
        print(f"  [!] Error parsing feed for {channel_name}: {e}")

    return articles_meta


def scrape_article_details(url, existing_author=""):
    """
    Visits the full article URL to scrape the full article body text and author.
    """
    if not url or url == "N/A" or not url.startswith("http"):
        return existing_author or "Unknown", "Content unavailable (invalid URL)"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        if resp.status_code != 200:
            return existing_author or "Unknown", f"HTTP {resp.status_code} Error"

        soup = BeautifulSoup(resp.content, "html.parser")

        # 1. Author Extraction (if not already found in RSS)
        author = existing_author
        if not author:
            # Check meta tags
            author_meta = (
                soup.find("meta", attrs={"name": "author"})
                or soup.find("meta", attrs={"property": "article:author"})
                or soup.find("meta", attrs={"name": "byl"})
            )
            if author_meta and author_meta.get("content"):
                author = author_meta["content"].strip()
            else:
                # Common byline classes
                byline_tag = soup.find(class_=lambda c: c and "byline" in c.lower())
                if byline_tag:
                    author = byline_tag.get_text(strip=True)
                else:
                    author = "Staff Writer"

        # 2. Content Extraction
        # Look for article container first, otherwise all <p> tags
        article_body = soup.find("article") or soup.find("main") or soup
        paragraphs = article_body.find_all("p")

        clean_paras = []
        for p in paragraphs:
            text = p.get_text(strip=True)
            # Filter out cookies, disclaimers, or very short snippets
            if len(text) > 35 and "cookie" not in text.lower() and "rights reserved" not in text.lower():
                clean_paras.append(text)

        full_content = "\n\n".join(clean_paras) if clean_paras else "No content paragraphs extracted."
        return author, full_content

    except Exception as e:
        return existing_author or "Unknown", f"Error scraping article: {e}"


def init_csv_file(filepath):
    """Initializes the CSV file with headers if it does not already exist."""
    try:
        # Open with utf-8-sig so Excel on Windows opens it with correct formatting
        with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()
    except PermissionError:
        print(f"\n[ERROR] Cannot open '{filepath}' for writing!")
        print("  -> It is currently OPEN in Microsoft Excel or another program.")
        print("  -> Please CLOSE the file in Excel and run the script again.\n")
        sys.exit(1)


def append_article_to_csv(filepath, article_data):
    """Appends a single scraped article row to the CSV file immediately."""
    with open(filepath, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writerow(article_data)


def main():
    print("=" * 70)
    print(" MULTI-CHANNEL NEWS SCRAPER (HYBRID RSS + HTML PARSER)")
    print("=" * 70)

    feeds = load_feeds(FEEDS_CONFIG)
    print(f"[*] Loaded {len(feeds)} news channels from {FEEDS_CONFIG}")
    print(f"[*] Target CSV: {CSV_FILE}")
    print(f"[*] Articles per channel: {ARTICLES_PER_CHANNEL} (Total goal: {len(feeds) * ARTICLES_PER_CHANNEL} articles)")
    print("-" * 70)

    # Step 1: Initialize CSV file with headers
    init_csv_file(CSV_FILE)

    total_scraped = 0

    # Step 2: Loop through each channel
    for channel_idx, (channel_name, feed_url) in enumerate(feeds.items(), start=1):
        print(f"\n[{channel_idx}/{len(feeds)}] Fetching feed for: {channel_name}...")
        articles_meta = fetch_channel_articles(channel_name, feed_url, max_articles=ARTICLES_PER_CHANNEL)
        print(f"    Found {len(articles_meta)} articles in feed.")

        # Step 3: Loop through each article in this channel
        for art_idx, meta in enumerate(articles_meta, start=1):
            title_display = meta['title'][:55] + "..." if len(meta['title']) > 55 else meta['title']
            print(f"    [{art_idx}/{len(articles_meta)}] Scraping: {title_display}")

            # Scrape deep details (author & full body content)
            author, content = scrape_article_details(meta["url"], existing_author=meta["author"])

            article_record = {
                "channel_name": meta["channel_name"],
                "title": meta["title"],
                "url": meta["url"],
                "time": meta["time"],
                "author": author,
                "summary": meta["summary"],
                "content": content
            }

            # Step 4: Stream immediately to CSV
            append_article_to_csv(CSV_FILE, article_record)
            total_scraped += 1

            # Polite delay between requests
            time.sleep(REQUEST_DELAY_SECONDS)

    print("\n" + "=" * 70)
    print(f"[SUCCESS] Scraping completed! Successfully saved {total_scraped} articles to: {CSV_FILE}")
    print("=" * 70)


if __name__ == "__main__":
    main()
