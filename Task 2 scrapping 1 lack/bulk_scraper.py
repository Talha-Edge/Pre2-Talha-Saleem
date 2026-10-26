"""
================================================================================
BULK NEWS COLLECTOR  (target: 100,000 articles for a machine learning dataset)
================================================================================
IDEA IN ONE PICTURE
    Many sources run AT THE SAME TIME. Inside one source only a few threads run.
    A rate limiter guarantees no single website ever gets more than N requests/second,
    however many threads you add.

        +-- web sources     (live sites: sitemap -> articles)   3 threads per site   <- THE SCRAPER (default)
        +-- Common Crawl    (public archive of the web)         optional, OFF by default
        +-- datasets        (ready-made news data, Hugging Face) optional, OFF by default
        every article -> output_bulk/data/<source>.jsonl  (one JSON per line, saved at once)

WHY THREADS HELP HERE
    Scraping is mostly WAITING for the network. While one thread waits, others work.
    But more threads on ONE website = blocked. So the speed comes from MANY sources, and
    from sources (Common Crawl, datasets) that are built to be downloaded fast.

COMMANDS
    python bulk_scraper.py plan                  # what will run + time estimate (no downloads)
    python bulk_scraper.py discover              # how many article URLs each site really offers
    python bulk_scraper.py run --smoke           # tiny test: 20 articles per source
    python bulk_scraper.py run                   # the real run (can be stopped with Ctrl+C and resumed)
    python bulk_scraper.py run --only cnn        # one channel / one source type (web, cc, ds)
    python bulk_scraper.py status                # how many articles each source has
    python bulk_scraper.py merge                 # combine everything into ONE csv (removes duplicates)

INSTALL
    pip install requests trafilatura feedparser warcio datasets
================================================================================
"""

import os
import re
import io
import sys
import csv
import json
import gzip
import time
import html as htmlmod
import random
import hashlib
import logging
import argparse
import threading
import urllib.parse
from collections import deque
from datetime import datetime
from functools import partial
from urllib.robotparser import RobotFileParser
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import trafilatura

try:
    from curl_cffi import requests as curl_requests
    CURL_CFFI_OK = True
except ImportError:
    curl_requests = None
    CURL_CFFI_OK = False

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# ==============================================================================
# SECTION 1: PATHS AND SETTINGS
# ==============================================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCES_FILE = os.path.join(BASE_DIR, "sources.json")
OUT_DIR = os.path.join(BASE_DIR, "output_bulk")
DATA_DIR = os.path.join(OUT_DIR, "data")
FINAL_DIR = os.path.join(OUT_DIR, "final")
SUMMARY_FILE = os.path.join(OUT_DIR, "summary.json")
URLS_DIR = os.path.join(OUT_DIR, "urls")
LOG_FILE = os.path.join(OUT_DIR, "bulk.log")

BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36")

MAX_SITEMAPS_READ = 400         # how many sitemap files to read per website (override: "max_sitemaps")
BREAKER_BLOCKS = 15             # this many blocks in a row -> stop that website
BREAKER_ERRORS = 30             # this many network errors in a row -> stop that website
PAYWALL_AFTER = 500              # after this many attempts...
PAYWALL_RATIO = 0.98             # ...if >98% were too short, the site is paywalled: stop it

CSV_COLUMNS = ["source_type", "source", "channel", "url", "title", "author", "date", "text", "words",
               "scraped_at"]

log = logging.getLogger("bulk")


def setup_logging():
    os.makedirs(OUT_DIR, exist_ok=True)
    if not log.handlers:
        log.setLevel(logging.INFO)
        h = logging.FileHandler(LOG_FILE, encoding="utf-8")
        h.setFormatter(logging.Formatter("[%(asctime)s] %(message)s"))
        log.addHandler(h)


def load_sources(path=SOURCES_FILE):
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    cfg.setdefault("global", {})
    cfg.setdefault("web_sources", [])
    cfg.setdefault("commoncrawl", {"enabled": False, "domains": []})
    cfg.setdefault("datasets", [])
    g = cfg["global"]
    g.setdefault("target_total", 100000)
    g.setdefault("min_words", 200)
    g.setdefault("web_default_rps", 1.0)
    g.setdefault("cc_threads", 12)
    g.setdefault("cc_rps", 8)
    g.setdefault("progress_every_sec", 30)
    g.setdefault("contact", "your-email@example.com")
    return cfg


# ==============================================================================
# SECTION 2: URL HELPERS
# ==============================================================================

SKIP_SEGMENTS = {
    "tag", "tags", "author", "authors", "category", "categories", "video", "videos", "gallery",
    "galleries", "live-blog", "liveblog", "live-news", "live", "photos", "photo", "audio", "watch",
    "podcast", "podcasts", "search", "login", "subscribe", "about", "contact", "privacy", "terms",
    "sitemap", "profile", "profiles", "topic", "topics", "newsletter", "newsletters", "shows",
}


def normalize_url(url):
    """Remove tracking junk and the trailing slash so duplicate links compare equal."""
    try:
        p = urllib.parse.urlparse(url.strip())
        q = [(k, v) for k, v in urllib.parse.parse_qsl(p.query, keep_blank_values=True)
             if not k.lower().startswith("utm_") and k.lower() not in ("fbclid", "gclid", "ocid")]
        return urllib.parse.urlunparse((p.scheme, p.netloc.lower(), p.path.rstrip("/") or "/",
                                        p.params, urllib.parse.urlencode(q), ""))
    except Exception:
        return url


def host_of(url):
    h = urllib.parse.urlparse(url).netloc.lower().split(":")[0]
    return h[4:] if h.startswith("www.") else h


def site_key(url):
    """edition.cnn.com and www.cnn.com -> 'cnn.com' (one rate limit for the whole site)."""
    labels = host_of(url).split(".")
    if len(labels) >= 3 and labels[-2] in ("co", "com", "org", "gov", "net", "go"):
        return ".".join(labels[-3:])
    return ".".join(labels[-2:])


def same_site(a, b):
    return site_key(a) == site_key(b)


def is_article_url(url, strict=True):
    """Does this link LOOK like one news article (not a section, tag or video page)?"""
    p = urllib.parse.urlparse(url)
    if p.scheme not in ("http", "https"):
        return False
    if re.search(r"\.(jpg|jpeg|png|gif|webp|svg|pdf|mp4|mp3|css|js|xml|zip)$", p.path, re.I):
        return False
    segs = [s.lower() for s in p.path.strip("/").split("/") if s]
    if not segs or len(p.path.strip("/")) < 5 or any(s in SKIP_SEGMENTS for s in segs):
        return False
    if not strict:
        return True
    if any(s.count("-") >= 3 for s in segs):
        return True
    if re.search(r"\d{5,}", p.path):
        return True
    if re.search(r"/(19|20)\d{2}/\d{1,2}(/\d{1,2})?/", "/" + p.path.strip("/") + "/"):
        return True
    if re.search(r"(^|&)(id|story_id)=\d{5,}", p.query):
        return True
    if len(segs) >= 2 and segs[-2] in ("article", "articles", "story", "stories", "a") and len(segs[-1]) >= 6:
        return True
    return False


# ==============================================================================
# SECTION 3: THE RATE LIMITER (the most important safety part)
# ==============================================================================

class DomainLimiter:
    """
    Gives each website its own 'speed limit', shared by ALL threads.
    A thread that wants to send a request asks wait(site). The limiter hands out time slots
    one interval apart, so 50 threads still send at most `rps` requests per second to that site.
    If the site pushes back (429/503) the limit is lowered automatically (slow_down),
    and it creeps back up while everything is fine (speed_up).
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._next = {}
        self._interval = {}
        self._base = {}

    def set_rate(self, site, rps):
        iv = 1.0 / max(float(rps), 0.02)
        with self._lock:
            self._base[site] = iv
            self._interval[site] = iv

    def wait(self, site):
        with self._lock:
            now = time.monotonic()
            iv = self._interval.get(site, 1.0)
            slot = max(now, self._next.get(site, 0.0))
            self._next[site] = slot + iv * random.uniform(1.0, 1.3)     # small random jitter
        delay = slot - now
        if delay > 0:
            time.sleep(delay)

    def slow_down(self, site, factor=1.5, cap=20.0):
        with self._lock:
            self._interval[site] = min(self._interval.get(site, 1.0) * factor, cap)

    def speed_up(self, site):
        with self._lock:
            base = self._base.get(site, 1.0)
            self._interval[site] = max(base, self._interval.get(site, base) * 0.98)

    def interval(self, site):
        with self._lock:
            return self._interval.get(site, 1.0)


# ==============================================================================
# SECTION 4: DOWNLOADING, BLOCK DETECTION, EXTRACTION
# ==============================================================================

_tls = threading.local()


def get_session():
    """One requests.Session per thread (sessions are not safe to share between threads)."""
    if not hasattr(_tls, "session"):
        _tls.session = requests.Session()
    return _tls.session


def fetch(url, limiter, ua, site=None, timeout=25, attempts=2, headers=None):
    """
    Polite download: waits for its turn from the limiter, retries on errors,
    backs off on 429/503 (and honours Retry-After). Returns a Response or None.
    NOTE: a Response is 'falsy' for status 400+, so always test `resp is None`.
    """
    site = site or site_key(url)
    hdrs = {"User-Agent": ua, "Accept-Language": "en-US,en;q=0.9"}
    if headers:
        hdrs.update(headers)
    last = None
    for attempt in range(attempts):
        limiter.wait(site)
        try:
            r = get_session().get(url, headers=hdrs, timeout=timeout)
            if r.status_code in (403, 401, 429) and CURL_CFFI_OK:
                try:
                    cr = curl_requests.get(url, headers=hdrs, timeout=timeout, impersonate="chrome120")
                    if cr.status_code == 200:
                        r = cr
                except Exception:
                    pass
        except requests.RequestException:
            if CURL_CFFI_OK:
                try:
                    cr = curl_requests.get(url, headers=hdrs, timeout=timeout, impersonate="chrome120")
                    if cr.status_code == 200:
                        limiter.speed_up(site)
                        return cr
                except Exception:
                    pass
            time.sleep(1 + 2 * attempt)
            continue
        last = r
        if r.status_code in (429, 503):
            limiter.slow_down(site)
            ra = r.headers.get("Retry-After", "")
            time.sleep(min(int(ra), 30) if ra.isdigit() else 2 * (attempt + 1))
            continue
        limiter.speed_up(site)
        return r
    return last


CHALLENGE_PHRASES = ("verify you are human", "just a moment", "checking your browser", "captcha",
                     "access denied", "attention required", "enable javascript and cookies",
                     "not available in your region", "not available in your country")


def classify(resp):
    """-> (kind, detail). kind: ok | blocked | gone | error.  A challenge page is a BLOCK, not an article."""
    if resp is None:
        return "error", "NO_RESPONSE"
    sc = resp.status_code
    if sc in (403, 429, 451, 503):
        return "blocked", "HTTP_%d" % sc
    if sc in (404, 410):
        return "gone", "HTTP_%d" % sc
    if sc not in (200, 206):
        return "error", "HTTP_%d" % sc
    body = resp.content
    if len(body) < 15000:                      # a challenge page is always short
        low = body.decode("utf-8", "ignore").lower()
        if any(p in low for p in CHALLENGE_PHRASES):
            return "blocked", "CHALLENGE_PAGE"
    return "ok", ""


def extract_text(html):
    """Main article text (trafilatura ignores menus, ads, comments). html may be str or bytes."""
    try:
        return trafilatura.extract(html, include_comments=False, include_tables=False)
    except Exception:
        return None


def extract_meta(html):
    """title / author / date (slower, so only called for articles that passed the length test)."""
    try:
        m = trafilatura.extract_metadata(html)
    except Exception:
        m = None
    g = lambda k: (getattr(m, k, None) or "") if m else ""
    return str(g("title")), str(g("author")), str(g("date"))


def make_record(source_type, source, channel, url, title, author, date, text):
    return {"source_type": source_type, "source": source, "channel": channel, "url": url,
            "title": title or "", "author": author or "", "date": str(date or ""),
            "text": text, "words": len(text.split()), "scraped_at": datetime.now().isoformat(timespec="seconds")}


# ==============================================================================
# SECTION 5: STORAGE (one JSON line per article, saved immediately)
# ==============================================================================

class Sink:
    """
    Appends articles to output_bulk/data/<name>.jsonl. Why JSON lines? Appending one line is
    safe and fast, a crash loses at most the last line, and resuming = reading the file again
    to learn which URLs are already done.
    """

    def __init__(self, name):
        os.makedirs(DATA_DIR, exist_ok=True)
        self.path = os.path.join(DATA_DIR, name.replace(":", "_") + ".jsonl")
        self.lock = threading.Lock()
        self.seen = set()
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        self.seen.add(normalize_url(json.loads(line)["url"]))
                    except Exception:
                        pass                                    # half-written last line: ignore
            with open(self.path, "rb") as f:                    # make sure the file ends with a newline
                f.seek(0, os.SEEK_END)
                if f.tell() > 0:
                    f.seek(-1, os.SEEK_END)
                    needs_newline = f.read(1) != b"\n"
                else:
                    needs_newline = False
        else:
            needs_newline = False
        self.count = len(self.seen)
        self.fh = open(self.path, "a", encoding="utf-8")
        if needs_newline:
            self.fh.write("\n")

    def has(self, url):
        return normalize_url(url) in self.seen

    def add(self, rec):
        key = normalize_url(rec["url"])
        with self.lock:
            if key in self.seen:
                return False
            self.fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            self.fh.flush()
            self.seen.add(key)
            self.count += 1
        return True

    def close(self):
        try:
            self.fh.close()
        except Exception:
            pass


# ==============================================================================
# SECTION 6: PROGRESS, STOPPING, AND THE LIVE REPORT
# ==============================================================================

def make_bar(current, target, width=12):
    if target <= 0:
        return "[" + " " * width + "]   0.0%"
    pct = min(max(float(current) / float(target), 0.0), 1.0)
    filled = int(round(width * pct))
    try:
        bar = "█" * filled + "░" * (width - filled)
    except Exception:
        bar = "=" * filled + "-" * (width - filled)
    return f"[{bar}] {pct*100:5.1f}%"


class Progress:
    """Shared scoreboard. Every thread reports here; one monitor thread prints it."""

    def __init__(self, total_target):
        self.lock = threading.Lock()
        self.total_target = total_target
        self.counts, self.targets, self.status, self.fails = {}, {}, {}, {}
        self.total = 0
        self.start_total = 0
        self.t0 = time.time()
        self.stop = threading.Event()

    def register(self, name, already, target):
        with self.lock:
            self.counts[name] = already
            self.targets[name] = target
            self.status[name] = "starting"
            self.fails[name] = {}
            self.total += already
            self.start_total += already
            if self.total >= self.total_target:
                self.stop.set()

    def add(self, name):
        with self.lock:
            self.counts[name] += 1
            self.total += 1
            if self.total >= self.total_target:
                self.stop.set()

    def fail(self, name, reason):
        with self.lock:
            d = self.fails[name]
            d[reason] = d.get(reason, 0) + 1

    def set_status(self, name, msg):
        with self.lock:
            self.status[name] = msg
        log.info("[%s] %s", name, msg)

    def source_done(self, name):
        with self.lock:
            return self.counts.get(name, 0) >= self.targets.get(name, 0)

    def should_stop(self, name):
        return self.stop.is_set() or self.source_done(name)

    def report(self):
        with self.lock:
            el = max(time.time() - self.t0, 1)
            rate = (self.total - self.start_total) / el
            left = max(self.total_target - self.total, 0)
            eta = "%d:%02d:%02d" % (left / rate // 3600, left / rate % 3600 // 60, left / rate % 60) if rate > 0 else "?"
            tot_bar = make_bar(self.total, self.total_target, width=16)
            lines = ["", "=== %s | TOTAL %s (%s / %s) | %.1f articles/s | ETA %s ===" % (
                datetime.now().strftime("%H:%M:%S"), tot_bar, format(self.total, ","), format(self.total_target, ","), rate, eta)]
            for n in sorted(self.counts):
                sbar = make_bar(self.counts[n], self.targets[n], width=10)
                lines.append("  %-22s %s  %6d / %-6d  %s" % (n, sbar, self.counts[n], self.targets[n], self.status[n]))
            return "\n".join(lines)


def monitor(prog, every, done_event):
    while not done_event.wait(every):
        print(prog.report(), flush=True)


# ==============================================================================
# SECTION 7: SOURCE TYPE 1 - LIVE WEBSITES (sitemaps -> articles)
# ==============================================================================

def get_robots(base_url, limiter, ua):
    """robots.txt rules (cached by the caller). Returns (parser or None, sitemap list, crawl_delay)."""
    p = urllib.parse.urlparse(base_url)
    r = fetch(p.scheme + "://" + p.netloc + "/robots.txt", limiter, ua, site=site_key(base_url), timeout=10, attempts=1)
    if r is None or r.status_code != 200 or not r.text:
        return None, [], None
    if "<html" in r.text[:300].lower():
        return None, [], None
    lines = r.text.splitlines()
    rp = RobotFileParser()
    rp.parse(lines)
    sitemaps = [v.strip() for k, v in (l.split(":", 1) for l in lines if ":" in l)
                if k.strip().lower() == "sitemap" and v.strip().startswith("http")]
    try:
        delay = rp.crawl_delay("*")
    except Exception:
        delay = None
    return rp, sitemaps, (float(delay) if delay else None)


def parse_sitemap(text):
    """Returns ('index'|'urlset', [(url, lastmod), ...]). Uses plain regex: fast and safe on huge files."""
    kind = "index" if "<sitemapindex" in text[:3000].lower() else "urlset"
    tag = "sitemap" if kind == "index" else "url"
    out = []
    for block in re.findall(r"<%s>(.*?)</%s>" % (tag, tag), text, re.S | re.I):
        loc = re.search(r"<loc>\s*(.*?)\s*</loc>", block, re.S | re.I)
        if not loc:
            continue
        u = htmlmod.unescape(loc.group(1).replace("<![CDATA[", "").replace("]]>", "").strip())
        lm = re.search(r"<lastmod>\s*(.*?)\s*</lastmod>", block, re.S | re.I)
        out.append((u, lm.group(1).strip() if lm else ""))
    return kind, out


def read_sitemap_text(resp):
    content = resp.content
    if content[:2] == b"\x1f\x8b":
        try:
            content = gzip.decompress(content)
        except Exception:
            return ""
    return content.decode("utf-8", "ignore")


def gather_web_urls(src, limiter, robots_sitemaps, want):
    """Newest-first article URLs from sitemaps (and optional RSS feeds), de-duplicated."""
    base = src["url"]
    p = urllib.parse.urlparse(base)
    origin = p.scheme + "://" + p.netloc
    seeds = list(src.get("sitemaps") or []) or list(robots_sitemaps) or [origin + "/sitemap.xml"]
    queue, urls, seen, read = deque(seeds), [], set(), 0

    def add(link, strict):
        key = normalize_url(link)
        if key not in seen and same_site(link, base) and is_article_url(link, strict=strict):
            seen.add(key)
            urls.append(link)

    for feed in (src.get("rss") or []):
        r = fetch(feed, limiter, BROWSER_UA, site=site_key(base), timeout=15)
        if r is not None and r.status_code == 200:
            try:
                import feedparser
                for e in feedparser.parse(r.content).entries:
                    if getattr(e, "link", None):
                        add(e.link, strict=False)
            except Exception:
                pass

    max_read = int(src.get("max_sitemaps", MAX_SITEMAPS_READ))
    while queue and len(urls) < want and read < max_read:
        sm = queue.popleft()
        r = fetch(sm, limiter, BROWSER_UA, site=site_key(base), timeout=30)
        read += 1
        if r is None or r.status_code != 200:
            continue
        kind, entries = parse_sitemap(read_sitemap_text(r))
        if kind == "index":
            entries = [e for e in entries if "video" not in e[0].lower() and "image" not in e[0].lower()]
            entries.sort(key=lambda e: ("news" in e[0].lower(), e[1]), reverse=True)    # newest + 'news' first
            for link, _ in reversed(entries):
                queue.appendleft(link)                                                   # visit newest first
        else:
            entries.sort(key=lambda e: e[1], reverse=True)
            for link, _ in entries:
                add(link, strict=True)

    if len(urls) < min(want, 200):            # sitemap gave (almost) nothing: crawl homepage + sections
        for link in crawl_links(base, limiter):
            add(link, strict=True)
    return urls


def crawl_links(base, limiter, max_sections=15):
    """Fallback for sites without a usable sitemap: links from the homepage and its section pages."""
    def links_of(url):
        r = fetch(url, limiter, BROWSER_UA, site=site_key(base), timeout=20)
        if r is None or r.status_code != 200:
            return []
        txt = r.content.decode("utf-8", "ignore")
        return [urllib.parse.urljoin(url, htmlmod.unescape(h))
                for h in re.findall(r"""href=["']([^"'#]+)["']""", txt)]

    home = links_of(base)
    out = list(home)
    sections = []
    for l in home:
        segs = [x for x in urllib.parse.urlparse(l).path.strip("/").split("/") if x]
        if same_site(l, base) and 1 <= len(segs) <= 2 and is_article_url(l, strict=False) \
                and not is_article_url(l, strict=True) and l not in sections:
            sections.append(l)
    for sec in sections[:max_sections]:
        out += links_of(sec)
    return out


class WebState:
    """Counters that decide when to stop a website (circuit breaker + paywall detector)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.blocks = self.errors = self.attempts = self.shorts = 0
        self.stopped = None

    def event(self, kind):
        with self.lock:
            if kind == "ok":
                self.blocks = self.errors = 0
                self.attempts += 1
            elif kind == "short":
                self.blocks = self.errors = 0
                self.attempts += 1
                self.shorts += 1
                if self.attempts >= PAYWALL_AFTER and self.shorts / self.attempts > PAYWALL_RATIO and not self.stopped:
                    self.stopped = "PAYWALLED (almost every article is too short)"
            elif kind == "blocked":
                self.blocks += 1
                if self.blocks >= BREAKER_BLOCKS and not self.stopped:
                    self.stopped = "BLOCKED (%d blocks in a row)" % self.blocks
            elif kind == "error":
                self.errors += 1
                if self.errors >= BREAKER_ERRORS and not self.stopped:
                    self.stopped = "TOO MANY NETWORK ERRORS"


def web_worker(url, name, src, sink, prog, limiter, state, min_words):
    if prog.should_stop(name) or state.stopped:
        return
    site = site_key(src["url"])
    r = fetch(url, limiter, BROWSER_UA, site=site)
    kind, detail = classify(r)
    if kind == "blocked":
        limiter.slow_down(site)
        state.event("blocked"); prog.fail(name, detail); return
    if kind in ("gone", "error"):
        state.event("error" if kind == "error" else "ok"); prog.fail(name, detail); return
    html = r.content
    text = extract_text(html)
    if not text or len(text.split()) < min_words:
        state.event("short"); prog.fail(name, "TOO_SHORT_OR_PAYWALL"); return
    title, author, date = extract_meta(html)
    state.event("ok")
    if title and sink.add(make_record("web", src["name"], src["name"], url, title, author, date, text)):
        prog.add(name)


def prepare_web(src, cfg, limiter, on_status=lambda m: None):
    """
    Robots rules + speed limit + the list of article URLs for one website.
    The URL list is cached in output_bulk/urls/<name>.json, so `discover` and `run` share the work.
    Returns (robots_parser_or_None, requests_per_second, urls, from_cache).
    """
    g = cfg["global"]
    target = int(src.get("target", 1000))
    on_status("reading robots.txt")
    rp, robots_sitemaps, delay = get_robots(src["url"], limiter, BROWSER_UA)
    rps = float(src.get("rps", g["web_default_rps"]))
    if delay:
        rps = min(rps, 1.0 / max(delay, 0.1))
    limiter.set_rate(site_key(src["url"]), rps)

    cache = os.path.join(URLS_DIR, src["name"] + ".json")
    if os.path.exists(cache) and not g.get("refresh_urls"):
        try:
            with open(cache, "r", encoding="utf-8") as f:
                cached = json.load(f)
            if cached:
                return rp, rps, cached, True
        except Exception:
            pass
    on_status("reading sitemaps")
    urls = gather_web_urls(src, limiter, robots_sitemaps, want=target * 3)
    os.makedirs(URLS_DIR, exist_ok=True)
    with open(cache, "w", encoding="utf-8") as f:
        json.dump(urls, f)
    return rp, rps, urls, False


def run_web_source(src, cfg, prog, limiter):
    name = "web:" + src["name"]
    g = cfg["global"]
    target = int(src.get("target", 1000))
    sink = Sink(name)
    prog.register(name, sink.count, target)
    try:
        if prog.should_stop(name):
            prog.set_status(name, "done")
            return
        rp, rps, urls, _ = prepare_web(src, cfg, limiter, lambda m: prog.set_status(name, m))

        def allowed(u):
            try:
                return rp is None or rp.can_fetch("*", u)
            except Exception:
                return True

        todo = [u for u in urls if not sink.has(u) and allowed(u)]
        if not todo:
            prog.set_status(name, "NO URLS FOUND (site may block sitemaps) - Common Crawl/datasets cover it")
            return
        prog.set_status(name, "scraping (%d URLs, %.1f req/s, %d threads)" % (len(todo), rps, src.get("workers", 3)))
        state = WebState()
        with ThreadPoolExecutor(max_workers=int(src.get("workers", 3))) as pool:
            futs = [pool.submit(web_worker, u, name, src, sink, prog, limiter, state, g["min_words"]) for u in todo]
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as e:
                    log.error("[%s] worker error: %s", name, e)
        if state.stopped:
            prog.set_status(name, "STOPPED: " + state.stopped)
        else:
            prog.set_status(name, "done" if prog.source_done(name) else "finished (ran out of URLs)")
    except Exception as e:
        prog.set_status(name, "ERROR: %s" % e)
    finally:
        sink.close()


# ==============================================================================
# SECTION 8: SOURCE TYPE 2 - COMMON CRAWL (a public archive of the web)
# ==============================================================================
# Common Crawl is a non-profit that stores copies of billions of web pages and lets anybody
# download them. Step 1: ask the INDEX "which saved pages belong to cnn.com?" Step 2: download
# just that page's bytes (a 'range request') from the archive file and read it.

CC_INDEX_SITE = "cc-index"
CC_DATA_SITE = "cc-data"


def cc_ua(cfg):
    return "news-dataset-student-project (contact: %s)" % cfg["global"]["contact"]


def cc_latest_crawls(n, limiter, ua):
    r = fetch("https://index.commoncrawl.org/collinfo.json", limiter, ua, site=CC_INDEX_SITE, timeout=30)
    if r is None or r.status_code != 200:
        return []
    return [(c["id"], c["cdx-api"]) for c in r.json()[:n]]


def cc_index_query(cdx_api, domain, limit, limiter, ua):
    """Ask the index for pages of a domain. Returns a list of dicts: url, filename, offset, length."""
    params = [("url", domain), ("matchType", "domain"), ("output", "json"), ("limit", str(limit)),
              ("fl", "url,filename,offset,length"), ("filter", "status:200"), ("filter", "mime:text/html")]
    r = fetch(cdx_api + "?" + urllib.parse.urlencode(params), limiter, ua, site=CC_INDEX_SITE, timeout=120, attempts=3)
    if r is None or r.status_code != 200:
        return []
    out = []
    for line in r.text.splitlines():
        try:
            out.append(json.loads(line))
        except Exception:
            pass
    return out


def parse_warc_record(raw):
    """Bytes of ONE gzip-compressed WARC record -> (original_url, html_bytes) or None."""
    from warcio.archiveiterator import ArchiveIterator
    for rec in ArchiveIterator(io.BytesIO(raw)):
        if rec.rec_type != "response":
            continue
        return rec.rec_headers.get_header("WARC-Target-URI"), rec.content_stream().read()
    return None


def cc_worker(rec, name, channel, sink, prog, limiter, ua, min_words):
    if prog.should_stop(name):
        return
    try:
        off, ln = int(rec["offset"]), int(rec["length"])
        r = fetch("https://data.commoncrawl.org/" + rec["filename"], limiter, ua, site=CC_DATA_SITE, timeout=60,
                  headers={"Range": "bytes=%d-%d" % (off, off + ln - 1), "Accept-Encoding": "identity"})
        if r is None:
            prog.fail(name, "NO_RESPONSE"); return
        if r.status_code != 206:                          # 206 = "here is the part you asked for"
            prog.fail(name, "HTTP_%d" % r.status_code); return
        parsed = parse_warc_record(r.content)
        if not parsed:
            prog.fail(name, "BAD_WARC"); return
        url, html = parsed
        text = extract_text(html)
        if not text or len(text.split()) < min_words:
            prog.fail(name, "TOO_SHORT_OR_PAYWALL"); return
        title, author, date = extract_meta(html)
        if title and sink.add(make_record("commoncrawl", name, channel, url or rec["url"], title, author, date, text)):
            prog.add(name)
    except Exception as e:
        prog.fail(name, "ERROR_" + type(e).__name__)


def run_cc_domain(dom, crawls, cfg, prog, limiter, threads):
    name = "cc:" + dom["name"]
    g = cfg["global"]
    ua = cc_ua(cfg)
    target = int(dom.get("target", 1000))
    sink = Sink(name)
    prog.register(name, sink.count, target)
    try:
        if prog.should_stop(name):
            prog.set_status(name, "done")
            return
        prog.set_status(name, "asking the index")
        per = min(max(100, target * 3 // max(len(crawls), 1)), 50000)
        recs, seen = [], set()
        for _, api in crawls:
            for r in cc_index_query(api, dom["domain"], per, limiter, ua):
                u = r.get("url", "")
                key = normalize_url(u)
                if key not in seen and is_article_url(u, strict=True) and not sink.has(u):
                    seen.add(key)
                    recs.append(r)
        random.shuffle(recs)                              # mix the crawls / sections
        if not recs:
            prog.set_status(name, "index returned nothing")
            return
        prog.set_status(name, "downloading (%d candidates)" % len(recs))
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futs = [pool.submit(cc_worker, r, name, dom["name"], sink, prog, limiter, ua, g["min_words"]) for r in recs]
            for f in as_completed(futs):
                try:
                    f.result()
                except Exception as e:
                    log.error("[%s] worker error: %s", name, e)
        prog.set_status(name, "done" if prog.source_done(name) else "finished (ran out of candidates)")
    except Exception as e:
        prog.set_status(name, "ERROR: %s" % e)
    finally:
        sink.close()


# ==============================================================================
# SECTION 9: SOURCE TYPE 3 - READY-MADE DATASETS (Hugging Face, streaming)
# ==============================================================================

def run_dataset_source(src, cfg, prog):
    name = "ds:" + src["name"]
    g = cfg["global"]
    target = int(src.get("target", 1000))
    sink = Sink(name)
    prog.register(name, sink.count, target)
    try:
        if prog.should_stop(name):
            prog.set_status(name, "done")
            return
        try:
            from datasets import load_dataset
        except ImportError:
            prog.set_status(name, "FAILED: run  pip install datasets")
            return
        prog.set_status(name, "opening dataset %s (streaming)" % src["hf_id"])
        try:
            ds = load_dataset(src["hf_id"], src.get("config"), split=src.get("split", "train"), streaming=True)
        except Exception as e:
            prog.set_status(name, "FAILED to open: %s" % str(e)[:120])
            return
        prog.set_status(name, "reading rows")
        field = lambda row, key: (row.get(src[key]) if src.get(key) else None)
        for i, row in enumerate(ds):
            if prog.should_stop(name):
                break
            text = str(field(row, "text_field") or "").strip()
            if len(text.split()) < g["min_words"]:
                prog.fail(name, "TOO_SHORT"); continue
            url = field(row, "url_field") or "hf://%s/%s/%d" % (src["hf_id"], src.get("split", "train"), i)
            channel = field(row, "channel_field") or src.get("channel") or src["name"]
            rec = make_record("dataset", src["name"], channel, str(url), field(row, "title_field"), "",
                              field(row, "date_field"), text)
            if sink.add(rec):
                prog.add(name)
        prog.set_status(name, "done" if prog.source_done(name) else "finished (dataset ended)")
    except Exception as e:
        prog.set_status(name, "ERROR: %s" % e)
    finally:
        sink.close()


# ==============================================================================
# SECTION 10: COMMANDS (plan / run / status / merge)
# ==============================================================================

def selected(only, kind, short):
    """--only matches a source type (web, cc, ds) or a channel name (cnn)."""
    return not only or only in (kind, short, "%s:%s" % (kind, short))


def cmd_plan(cfg, args):
    g = cfg["global"]
    print("\n" + "=" * 78)
    print(" PLAN (no downloads happen here)")
    print("=" * 78)
    total, longest = 0, 0
    rows = []
    for s in cfg["web_sources"]:
        if not s.get("enabled", True) or not selected(args.only, "web", s["name"]):
            continue
        rps = float(s.get("rps", g["web_default_rps"]))
        secs = (s["target"] / 0.6) / rps                        # assume ~60% of pages succeed
        rows.append(("web:" + s["name"], s["target"], "%.1f req/s, %d threads" % (rps, s.get("workers", 3)), secs))
    cc = cfg["commoncrawl"]
    if cc.get("enabled"):
        doms = [d for d in cc["domains"] if selected(args.only, "cc", d["name"])]
        tot = sum(d["target"] for d in doms)
        secs = (tot / 0.5) / g["cc_rps"]                        # all domains share one download speed
        for d in doms:
            rows.append(("cc:" + d["name"], d["target"], "shared %d req/s, ~%d threads total" % (g["cc_rps"], g["cc_threads"]),
                         secs))
    for d in cfg["datasets"]:
        if not d.get("enabled", True) or not selected(args.only, "ds", d["name"]):
            continue
        rows.append(("ds:" + d["name"], d["target"], "streaming, 1 thread", d["target"] / 300.0))
    for name, t, how, secs in rows:
        total += t
        longest = max(longest, secs)
        print("  %-24s target %6d   %-34s ~%s" % (name, t, how, time.strftime("%Hh %Mm", time.gmtime(secs))))
    print("-" * 78)
    print("  Sources add up to %s articles (goal %s)" % (format(total, ","), format(g["target_total"], ",")))
    print("  Everything runs in parallel, so wall-clock time is about the SLOWEST source: ~%s"
          % time.strftime("%Hh %Mm", time.gmtime(longest)))
    if total < g["target_total"]:
        print("  [!] The targets add up to LESS than the goal. Raise some targets in sources.json.")
    if total > g["target_total"] * 1.6:
        print("  [i] Targets are well above the goal. That is fine: failures and duplicates eat about 20-40%.")
    print("  Threads at peak: %d (web) + %d (Common Crawl) + %d (datasets)" % (
        sum(s.get("workers", 3) for s in cfg["web_sources"] if s.get("enabled", True)),
        (max(2, -(-int(g["cc_threads"]) // max(len(cc.get("domains", [])), 1))) * len(cc.get("domains", []))) if cc.get("enabled") else 0,
        len([d for d in cfg["datasets"] if d.get("enabled", True)])))
    print("  Estimates assume the sites answer normally. Blocks and paywalls will lower the real number.")
    print("=" * 78 + "\n")


def cmd_discover(cfg, args):
    """Reads ONLY robots.txt and sitemaps (no article downloads) and tells you how many articles each site can supply."""
    setup_logging()
    g = cfg["global"]
    g["refresh_urls"] = bool(getattr(args, "refresh", False))
    limiter = DomainLimiter()
    srcs = [s for s in cfg["web_sources"] if s.get("enabled", True) and selected(args.only, "web", s["name"])]
    if not srcs:
        print("[!] No web sources selected.")
        return
    print("\n[i] Discovering article URLs for %d sites (sitemaps only, no articles downloaded)..." % len(srcs))
    results = {}

    def job(src):
        try:
            _, rps, urls, cached = prepare_web(src, cfg, limiter)
            results[src["name"]] = (len(urls), cached, rps)
        except Exception as e:
            results[src["name"]] = (-1, False, 0)
            log.error("discover %s: %s", src["name"], e)

    with ThreadPoolExecutor(max_workers=len(srcs)) as pool:
        list(pool.map(job, srcs))

    print("\n%-20s %9s %9s %12s   %s" % ("SITE", "URLS", "TARGET", "EXPECTED", "VERDICT"))
    print("-" * 78)
    exp_total = 0
    for s in srcs:
        n, cached, rps = results.get(s["name"], (-1, False, 0))
        expected = int(max(n, 0) * 0.6)                       # roughly 60% of pages turn into a valid article
        expected = min(expected, s["target"])
        exp_total += expected
        verdict = "FAILED to read" if n < 0 else ("enough" if expected >= s["target"] * 0.9 else
                                                  "LOW - add sitemaps/rss in sources.json or lower target")
        print("%-20s %9s %9s %12s   %s%s" % (s["name"], format(max(n, 0), ","), format(s["target"], ","),
                                             format(expected, ","), verdict, "  (cached)" if cached else ""))
    print("-" * 78)
    print("Expected total from live scraping: about %s articles (goal %s)\n" % (format(exp_total, ","),
                                                                              format(g["target_total"], ",")))


def cmd_run(cfg, args):
    setup_logging()
    g = cfg["global"]
    if args.cc_threads:
        g["cc_threads"] = args.cc_threads
    g["refresh_urls"] = bool(args.refresh)
    for s in cfg["web_sources"]:
        if args.web_workers:
            s["workers"] = args.web_workers
        if args.rps:
            s["rps"] = args.rps
    if args.smoke:
        for s in cfg["web_sources"]:
            s["target"] = 20
        for d in cfg["commoncrawl"].get("domains", []):
            d["target"] = 20
        for d in cfg["datasets"]:
            d["target"] = 20
        g["target_total"] = 10 ** 9                       # do not stop early in smoke mode
    if args.target:
        g["target_total"] = args.target

    prog = Progress(g["target_total"])
    limiter = DomainLimiter()
    limiter.set_rate(CC_INDEX_SITE, 1.0)
    limiter.set_rate(CC_DATA_SITE, g["cc_rps"])

    jobs = []
    for s in cfg["web_sources"]:
        if s.get("enabled", True) and selected(args.only, "web", s["name"]):
            jobs.append(partial(run_web_source, s, cfg, prog, limiter))
    cc = cfg["commoncrawl"]
    if cc.get("enabled"):
        doms = [d for d in cc["domains"] if selected(args.only, "cc", d["name"])]
        if doms:
            ua = cc_ua(cfg)
            crawls = cc_latest_crawls(int(cc.get("crawls_to_use", 3)), limiter, ua)
            if not crawls:
                print("[!] Could not read the Common Crawl index list (no internet or the service is busy).")
            else:
                print("[i] Common Crawl crawls used: %s" % ", ".join(c[0] for c in crawls))
                per_domain = max(2, -(-int(g["cc_threads"]) // len(doms)))    # share ONE thread budget
                for d in doms:
                    jobs.append(partial(run_cc_domain, d, crawls, cfg, prog, limiter, per_domain))
    for d in cfg["datasets"]:
        if d.get("enabled", True) and selected(args.only, "ds", d["name"]):
            jobs.append(partial(run_dataset_source, d, cfg, prog))

    if not jobs:
        print("[!] Nothing to run (check --only and the 'enabled' flags).")
        return
    print("\n[i] Starting %d sources in parallel. Ctrl+C stops safely; run again to resume.\n" % len(jobs))

    done = threading.Event()
    threading.Thread(target=monitor, args=(prog, g["progress_every_sec"], done), daemon=True).start()
    pool = ThreadPoolExecutor(max_workers=len(jobs))
    futs = [pool.submit(j) for j in jobs]
    try:
        for f in as_completed(futs):
            try:
                f.result()
            except Exception as e:
                log.error("source crashed: %s", e)
    except KeyboardInterrupt:
        print("\n[!] Stopping... finishing the articles in progress (everything saved so far is kept).")
        prog.stop.set()
        pool.shutdown(wait=True, cancel_futures=True)
    finally:
        done.set()
        pool.shutdown(wait=True)

    print(prog.report())
    summary = {n: {"articles": prog.counts[n], "target": prog.targets[n], "status": prog.status[n],
                   "problems": prog.fails[n]} for n in prog.counts}
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(SUMMARY_FILE, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    print("\nWhy articles were skipped (per source):")
    for n, s in sorted(summary.items()):
        if s["problems"]:
            top = sorted(s["problems"].items(), key=lambda kv: -kv[1])[:3]
            print("  %-26s %s" % (n, ", ".join("%s x%d" % kv for kv in top)))
    print("\nNext:  python bulk_scraper.py merge\n")


def count_lines(path):
    n = 0
    with open(path, "rb") as f:
        for _ in f:
            n += 1
    return n


def cmd_status(cfg, args):
    print("\n%-22s %-18s %9s" % ("SOURCE FILE", "PROGRESS", "ARTICLES"))
    total = 0
    target_map = {s["name"]: int(s.get("target", 1000)) for s in cfg.get("web_sources", [])}
    if os.path.isdir(DATA_DIR):
        for fn in sorted(os.listdir(DATA_DIR)):
            if fn.endswith(".jsonl"):
                sname = fn[:-6].replace("web_", "").replace("cc_", "").replace("ds_", "")
                target = target_map.get(sname, 1000)
                n = count_lines(os.path.join(DATA_DIR, fn))
                total += n
                bar = make_bar(n, target, width=10)
                print("%-22s %s %9s" % (fn[:-6], bar, format(n, ",")))
    print("-" * 52)
    tot_target = cfg["global"]["target_total"]
    tot_bar = make_bar(total, tot_target, width=12)
    print("%-22s %s %9s   (goal %s)\n" % ("TOTAL", tot_bar, format(total, ","), format(tot_target, ",")))


def text_fingerprint(text):
    """Same article text from two different sources -> same fingerprint."""
    t = re.sub(r"\W+", " ", text.lower())[:1500]
    return hashlib.md5(t.encode("utf-8", "ignore")).hexdigest()


def cmd_merge(cfg, args):
    """Stream every .jsonl into one CSV. Removes duplicate URLs AND duplicate texts (low memory)."""
    os.makedirs(FINAL_DIR, exist_ok=True)
    out_path = os.path.join(FINAL_DIR, "articles_bulk.csv")
    csv.field_size_limit(min(sys.maxsize, 2 ** 31 - 1))
    seen_urls, seen_text = set(), set()
    per_channel, dup_url, dup_text, total_in = {}, 0, 0, 0
    files = sorted(os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".jsonl")) \
        if os.path.isdir(DATA_DIR) else []
    if not files:
        print("[!] No data yet. Run:  python bulk_scraper.py run")
        return
    try:
        out_f = open(out_path, "w", newline="", encoding="utf-8-sig")
    except PermissionError:
        out_path = os.path.join(FINAL_DIR, "articles_bulk_new.csv")
        out_f = open(out_path, "w", newline="", encoding="utf-8-sig")
        print("\n  [!] Notice: articles_bulk.csv was locked by another program. Saved to %s instead." % out_path)
    with out_f as out:
        w = csv.DictWriter(out, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        w.writeheader()
        for path in files:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    total_in += 1
                    key = normalize_url(rec.get("url", ""))
                    fp = text_fingerprint(rec.get("text", ""))
                    if key in seen_urls:
                        dup_url += 1; continue
                    if fp in seen_text:
                        dup_text += 1; continue
                    seen_urls.add(key)
                    seen_text.add(fp)
                    w.writerow(rec)
                    ch = rec.get("channel", "?")
                    per_channel[ch] = per_channel.get(ch, 0) + 1
    kept = sum(per_channel.values())
    print("\n" + "=" * 60)
    print(" MERGE REPORT")
    print("=" * 60)
    print("  Read %s rows | duplicate URLs %s | duplicate texts %s" % (format(total_in, ","), format(dup_url, ","),
                                                                      format(dup_text, ",")))
    for ch, n in sorted(per_channel.items(), key=lambda kv: -kv[1]):
        print("  %-24s %8s" % (ch, format(n, ",")))
    print("-" * 60)
    print("  KEPT %s articles -> %s" % (format(kept, ","), out_path))
    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="Bulk news collector (100,000 articles)")
    sub = parser.add_subparsers(dest="cmd")
    for name, helptext in [("plan", "show the plan and time estimate"), ("run", "collect articles"),
                           ("discover", "count how many article URLs each site offers"),
                           ("status", "count articles per source"), ("merge", "combine into one CSV")]:
        p = sub.add_parser(name, help=helptext)
        p.add_argument("--only", default=None, help="a source type (web, cc, ds) or a channel name (cnn)")
        if name == "run":
            p.add_argument("--smoke", action="store_true", help="quick test: 20 articles per source")
            p.add_argument("--target", type=int, default=None, help="override the total goal")
            p.add_argument("--cc-threads", type=int, default=None, help="threads for Common Crawl downloads")
            p.add_argument("--web-workers", type=int, default=None, help="threads PER website (default from sources.json)")
            p.add_argument("--rps", type=float, default=None, help="requests/second PER website (default 1.0)")
            p.add_argument("--refresh", action="store_true", help="ignore cached URL lists and read sitemaps again")
        if name == "discover":
            p.add_argument("--refresh", action="store_true", help="ignore cached URL lists and read sitemaps again")
    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        return
    cfg = load_sources()
    {"plan": cmd_plan, "discover": cmd_discover, "run": cmd_run, "status": cmd_status,
     "merge": cmd_merge}[args.cmd](cfg, args)


if __name__ == "__main__":
    main()
