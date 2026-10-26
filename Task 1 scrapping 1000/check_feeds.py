import json
import requests
from bs4 import BeautifulSoup

with open("Day1/feeds.json", "r") as f:
    feeds = json.load(f)

headers = {"User-Agent": "Mozilla/5.0"}

print("Checking feeds connectivity...")
for name, url in feeds.items():
    try:
        r = requests.get(url, headers=headers, timeout=6)
        soup = BeautifulSoup(r.content, "xml")
        items = soup.find_all("item")
        if not items:
            items = soup.find_all("entry")
        print(f"[{name}] -> Status {r.status_code} | Found {len(items)} articles")
    except Exception as e:
        print(f"[{name}] -> ERROR: {e}")
