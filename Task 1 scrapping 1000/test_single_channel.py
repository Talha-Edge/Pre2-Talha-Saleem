"""
Experiment 1: Step 1 - Single Channel Test
=========================================
This script demonstrates the Hybrid Approach:
1. Fetch BBC's RSS feed using requests.
2. Parse XML tags (<item>, <title>, <link>, <pubDate>, <description>) using BeautifulSoup.
3. Visit the article URL to scrape the full article body paragraphs.
"""

import requests
from bs4 import BeautifulSoup

# Step 1: Tell websites who is visiting (User-Agent header prevents simple bot blocking)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# BBC News public RSS feed URL
RSS_URL = "https://feeds.bbci.co.uk/news/rss.xml"

print(f"[*] Fetching RSS feed from: {RSS_URL}")
response = requests.get(RSS_URL, headers=HEADERS, timeout=10)

# Step 2: Parse the RSS Feed (XML format)
soup = BeautifulSoup(response.content, "xml")

# Step 3: Find all news items (<item> tags)
items = soup.find_all("item")
print(f"[+] Total articles found in feed: {len(items)}\n")

# Let's inspect the very first article (Item #1)
first_item = items[0]

channel_name = "BBC News"
title = first_item.find("title").get_text(strip=True) if first_item.find("title") else "N/A"
url = first_item.find("link").get_text(strip=True) if first_item.find("link") else "N/A"
pub_date = first_item.find("pubDate").get_text(strip=True) if first_item.find("pubDate") else "N/A"
summary = first_item.find("description").get_text(strip=True) if first_item.find("description") else "N/A"

print("=" * 60)
print(f"Channel : {channel_name}")
print(f"Title   : {title}")
print(f"Date    : {pub_date}")
print(f"URL     : {url}")
print(f"Summary : {summary}")
print("=" * 60)

# Step 4: Now visit the article URL to get the full body content and author!
print(f"\n[*] Visiting article URL to extract full content...")
article_resp = requests.get(url, headers=HEADERS, timeout=10)
article_soup = BeautifulSoup(article_resp.content, "html.parser")

# Extract author (BBC often puts author in meta tag or class containing 'byline')
author_meta = article_soup.find("meta", attrs={"name": "author"})
author = author_meta["content"] if author_meta and "content" in author_meta.attrs else "BBC News Desk"

# Extract all paragraphs (<p> tags)
paragraphs = [p.get_text(strip=True) for p in article_soup.find_all("p")]
# Filter out short or empty paragraphs
clean_paragraphs = [p for p in paragraphs if len(p) > 25]
full_content = "\n".join(clean_paragraphs)

print(f"Author  : {author}")
print(f"Paragraphs collected: {len(clean_paragraphs)}")
print("\n--- Article Content Preview (First 300 characters) ---")
print(full_content[:300] + "...")
print("=" * 60)
