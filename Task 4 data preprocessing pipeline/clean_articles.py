#!/usr/bin/env python3
"""
clean_articles.py - an 8-stage data cleaning pipeline for scraped articles.

Author: written for Talha Saleem (learning project)

THE 8 STAGES (your trainer's list)
  1. Ingestion        read source files, keep a raw copy + checksum
  2. Profiling        schema, types, nulls, distributions, examples
  3. Standardization  snake_case columns, ISO dates (UTC), normalized URLs
  4. Validation       URL / date / text rules -> flags (not silent deletes)
  5. Deduplication    exact (URL, content hash) + near duplicates (MinHash + LSH)
  6. Cleaning         strip HTML, decode entities, normalize unicode/whitespace,
                      optional boilerplate removal
  7. Quality          completeness score + "questionable row" flags
  8. Export           clean data, rejected/flagged rows, duplicate map, report

THE 3 PRINCIPLES (enforced in the code)
  P1. Raw data is never overwritten. Every change goes into a NEW column.
  P2. Rows are never deleted just for a missing field. Rules produce FLAGS and
      a reject_reason. Only rules listed in CONFIG["reject_rules"] move a row to
      the rejected file, and even then the row is saved with its reason.
  P3. Leakage: nothing here is "fitted" on data (no scalers, no TF-IDF). If you
      later train a model, split by time or source and fit only on the train part.

HOW TO RUN
  python clean_articles.py --input data/raw/articles.csv --sample 1000   # try small first
  python clean_articles.py --input data/raw/articles.csv                 # full run

Input formats: .csv  .json  .jsonl  .parquet  (or a folder containing them)
"""

import argparse
import hashlib
import html
import json
import logging
import re
import shutil
import stat
import time
import unicodedata
import zlib
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import numpy as np
import pandas as pd
from bs4 import BeautifulSoup

# =====================================================================
# CONFIG - every threshold lives here, nothing is hard-coded below.
# Change a number here, re-run, and the whole pipeline follows.
# =====================================================================
CONFIG = {
    # Folders
    "raw_dir": "data/raw_copy",
    "interim_dir": "data/interim",
    "clean_dir": "data/clean",
    "rejected_dir": "data/rejected",
    "reports_dir": "reports",
    "logs_dir": "logs",

    # Column names we look for (after snake_case). First match wins.
    # Add your own column names here if yours are different.
    "column_aliases": {
        "url": ["url", "link", "article_url", "source_url"],
        "title": ["title", "headline", "article_title"],
        "body": ["body", "text", "content", "article_text", "article", "full_text"],
        "published_date": ["published_date", "date", "published", "publish_date",
                           "pub_date", "published_at", "datetime"],
        "source": ["source", "site", "website", "domain", "publisher"],
        "author": ["author", "authors", "byline"],
    },

    # Values that secretly mean "missing" (compared in lowercase)
    "null_like_values": ["", "n/a", "na", "null", "none", "nan", "nil", "-", "--",
                         "undefined", "unknown"],

    # Validation rules
    "min_body_chars": 200,           # shorter than this -> flag short_body
    "min_title_chars": 5,
    "min_date": "1990-01-01",        # earlier than this -> flag invalid_date
    "allowed_schemes": ["http", "https"],

    # Which problems REJECT a row (moved to rejected file). Everything else
    # only FLAGS the row but keeps it in the clean file. (Principle P2)
    "reject_rules": ["empty_body", "invalid_url_and_no_body"],

    # URL standardization: query parameters starting with these are removed
    "tracking_param_prefixes": ["utm_", "fbclid", "gclid", "ref", "mc_", "igshid"],

    # Deduplication
    "dedup_shingle_size": 5,         # words per shingle
    "minhash_permutations": 64,      # more = more accurate, slower
    "lsh_bands": 16,                 # 16 bands x 4 rows = good for ~0.7+ similarity
    "near_dup_threshold": 0.80,      # estimated Jaccard >= this -> near duplicate
    "dedup_keep": "longest",         # "longest" | "earliest" | "first"
    "dedup_min_words": 30,           # skip near-dup check for tiny texts
    "drop_duplicates_from_clean": True,  # duplicates go to the duplicates file

    # Boilerplate removal (optional, Stage 6e)
    "remove_boilerplate": True,
    "boilerplate_line_share": 0.05,  # a line seen in >5% of a source's articles = boilerplate
    "boilerplate_min_articles": 20,  # need at least this many articles in a source
    "boilerplate_phrases": [
        "subscribe now", "sign up for our newsletter", "follow us on",
        "all rights reserved", "cookie policy", "accept cookies", "read more:",
        "advertisement", "click here to", "share this article", "related articles",
        "privacy policy", "terms of use",
    ],

    # Quality assessment
    "required_fields": ["url", "title", "body", "published_date"],
    "max_non_letter_ratio": 0.40,    # more non-letters than this -> flag
    "max_repeated_line_ratio": 0.30, # share of repeated lines -> flag
    "iqr_multiplier": 1.5,           # Day 43 IQR rule for text-length outliers
    "quality_good": 0.85,            # score >= this -> "good"
    "quality_review": 0.60,          # score >= this -> "review", else "poor"

    "random_seed": 42,
}

log = logging.getLogger("cleaner")


# =====================================================================
# SMALL HELPERS
# =====================================================================
def setup_logging(logs_dir: str) -> None:
    """Log to console AND to a file, so every run leaves a record."""
    Path(logs_dir).mkdir(parents=True, exist_ok=True)
    fmt = "%(asctime)s | %(levelname)s | %(message)s"
    logging.basicConfig(
        level=logging.INFO, format=fmt,
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(Path(logs_dir) / "pipeline.log", mode="a",
                                      encoding="utf-8")],
    )


def to_snake_case(name: str) -> str:
    """'Published Date' -> 'published_date', 'ArticleURL' -> 'article_url'."""
    name = str(name).strip()
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)   # camelCase split
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name)            # spaces/symbols -> _
    return name.strip("_").lower()


def sha256_of_file(path: Path) -> str:
    """Fingerprint of a file. If even 1 byte changes, the hash changes."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def to_seconds(dates: pd.Series) -> pd.Series:
    """Datetime -> number of seconds since 1970 (NaN where the date is missing)."""
    return (dates - pd.Timestamp("1970-01-01", tz="UTC")).dt.total_seconds()


def is_null_like(series: pd.Series, null_like: list) -> pd.Series:
    """True where a cell is NaN OR looks like a hidden missing value."""
    as_text = series.astype("string").str.strip().str.lower()
    return series.isna() | as_text.isin(null_like)


class StageTracker:
    """Remembers how many rows exist after every stage (for the report)."""

    def __init__(self):
        self.rows = []

    def record(self, stage: str, n_rows: int, note: str = ""):
        self.rows.append({"stage": stage, "rows": int(n_rows), "note": note})
        log.info("[%s] rows=%d %s", stage, n_rows, note)


# =====================================================================
# STAGE 1 - INGESTION
# =====================================================================
def read_any_file(path: Path) -> pd.DataFrame:
    """Read csv/json/jsonl/parquet. Everything is read as text first
    (dtype=str) so pandas does not silently 'fix' or damage values."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        # utf-8-sig removes the invisible BOM some Excel files add.
        return pd.read_csv(path, dtype=str, encoding="utf-8-sig",
                           keep_default_na=False, on_bad_lines="warn")
    if suffix == ".jsonl":
        return pd.read_json(path, lines=True, dtype=False)
    if suffix == ".json":
        return pd.read_json(path, dtype=False)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported file type: {path}")


def stage1_ingest(input_path: str, sample: int | None, tracker: StageTracker):
    """Read files, make a READ-ONLY raw copy, hash it, add metadata columns."""
    log.info("=== STAGE 1: INGESTION ===")
    src = Path(input_path)
    files = ([src] if src.is_file() else
             sorted(p for p in src.rglob("*")
                    if p.suffix.lower() in {".csv", ".json", ".jsonl", ".parquet"}))
    if not files:
        raise FileNotFoundError(f"No readable files found at {input_path}")

    raw_dir = Path(CONFIG["raw_dir"])
    raw_dir.mkdir(parents=True, exist_ok=True)
    frames, checksums = [], {}
    for f in files:
        copy_path = raw_dir / f.name
        if not copy_path.exists():
            shutil.copy2(f, copy_path)
            copy_path.chmod(stat.S_IREAD)   # make the copy read-only (P1)
        checksums[f.name] = sha256_of_file(copy_path)
        df_part = read_any_file(f)
        df_part["source_file"] = f.name
        frames.append(df_part)
        log.info("Read %s -> %d rows", f.name, len(df_part))

    df = pd.concat(frames, ignore_index=True)
    rows_in_files = len(df)

    df.insert(0, "row_id", np.arange(len(df)))   # stable unique ID for every row
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()

    if sample:
        df = df.sample(n=min(sample, len(df)),
                       random_state=CONFIG["random_seed"]).sort_values("row_id")
        df = df.reset_index(drop=True)
        log.info("SAMPLE MODE: using %d rows", len(df))

    Path(CONFIG["reports_dir"]).mkdir(parents=True, exist_ok=True)
    with open(Path(CONFIG["reports_dir"]) / "raw_checksums.json", "w") as fh:
        json.dump(checksums, fh, indent=2)

    tracker.record("1_ingestion", len(df), f"loaded from {len(files)} file(s)")
    return df, {"files": [f.name for f in files], "checksums": checksums,
                "rows_in_files": rows_in_files}


# =====================================================================
# STAGE 2 - PROFILING
# =====================================================================
def stage2_profile(df: pd.DataFrame) -> dict:
    """Look before you touch. Nothing is changed here."""
    log.info("=== STAGE 2: PROFILING ===")
    prof = {"n_rows": len(df), "n_columns": df.shape[1],
            "memory_mb": round(df.memory_usage(deep=True).sum() / 1e6, 2)}

    schema = []
    for col in df.columns:
        s = df[col]
        hidden = is_null_like(s, CONFIG["null_like_values"])
        schema.append({
            "column": col,
            "dtype": str(s.dtype),
            "real_nulls": int(s.isna().sum()),
            "hidden_or_real_nulls": int(hidden.sum()),
            "null_pct": round(100 * hidden.mean(), 2),
            "unique_values": int(s.nunique(dropna=True)),
            "example": str(s.dropna().iloc[0])[:80] if s.notna().any() else "",
        })
    prof["schema"] = schema

    # Text length distribution (words + characters) for any text-like column
    text_stats = {}
    for col in df.columns:
        if col in ("row_id", "ingested_at", "source_file"):
            continue
        lens = df[col].astype("string").str.len().dropna()
        if len(lens):
            text_stats[col] = {k: float(v) for k, v in
                               lens.describe(percentiles=[.05, .25, .5, .75, .95]).items()}
    prof["length_stats"] = text_stats
    log.info("Profile: %d rows, %d cols, %.1f MB", prof["n_rows"],
             prof["n_columns"], prof["memory_mb"])
    return prof


# =====================================================================
# STAGE 3 - STANDARDIZATION (writes NEW columns, keeps the originals)
# =====================================================================
def normalize_url(url) -> str | None:
    """lowercase scheme+host, drop #fragment, strip tracking params,
    sort remaining params, remove trailing slash."""
    if url is None or (isinstance(url, float) and np.isnan(url)):
        return None
    url = str(url).strip()
    if not url:
        return None
    if "://" not in url:
        url = "http://" + url           # 'example.com/a' -> add a scheme
    try:
        parts = urlsplit(url)
    except ValueError:
        return None
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
             if not any(k.lower().startswith(p) for p in CONFIG["tracking_param_prefixes"])]
    query.sort()
    path = parts.path.rstrip("/") or "/"
    netloc = parts.netloc.lower()
    if netloc.startswith("www."):
        netloc = netloc[4:]
    return urlunsplit((parts.scheme.lower(), netloc, path, urlencode(query), ""))


def stage3_standardize(df: pd.DataFrame, tracker: StageTracker) -> pd.DataFrame:
    log.info("=== STAGE 3: STANDARDIZATION ===")

    # 3a. Column names -> snake_case. (Row ID etc. are already snake_case.)
    df.columns = [to_snake_case(c) for c in df.columns]

    # 3b. Map our messy column names to standard names (url, title, body...).
    #     We CREATE new columns with prefix raw_ so the original is preserved.
    resolved = {}
    for std_name, aliases in CONFIG["column_aliases"].items():
        found = next((a for a in aliases if a in df.columns), None)
        resolved[std_name] = found
        if found:
            df[f"raw_{std_name}"] = df[found]       # P1: untouched copy
        else:
            df[f"raw_{std_name}"] = pd.NA
            log.warning("No column found for '%s' (looked for %s)", std_name, aliases)
    log.info("Column mapping: %s", resolved)

    # 3c. Turn hidden-missing values ("N/A", "null", "  ") into real nulls
    for std_name in resolved:
        raw = df[f"raw_{std_name}"]
        mask = is_null_like(raw, CONFIG["null_like_values"])
        df[std_name] = raw.astype("string").str.strip().mask(mask, pd.NA)

    # 3d. URL normalization
    df["url_norm"] = df["url"].map(normalize_url)
    df["domain"] = df["url_norm"].map(
        lambda u: urlsplit(u).netloc if isinstance(u, str) else None)

    # 3e. Dates -> ISO 8601, in UTC. Unparseable dates become NaT (flagged later).
    parsed = pd.to_datetime(df["published_date"], errors="coerce", utc=True, format="mixed")
    df["date_utc"] = parsed
    df["date_iso"] = parsed.dt.strftime("%Y-%m-%d")

    # 3f. Source / author: tidy whitespace and case
    df["source"] = df["source"].str.replace(r"\s+", " ", regex=True).str.strip()
    # if no source column exists, fall back to the URL's domain
    df["source"] = df["source"].fillna(df["domain"])
    df["source_std"] = df["source"].str.lower()
    df["author"] = df["author"].str.replace(r"\s+", " ", regex=True).str.strip()

    tracker.record("3_standardization", len(df), "new columns added, no rows removed")
    return df


# =====================================================================
# STAGE 4 - VALIDATION (flags only; nothing is deleted)
# =====================================================================
def stage4_validate(df: pd.DataFrame, tracker: StageTracker) -> pd.DataFrame:
    log.info("=== STAGE 4: VALIDATION ===")
    c = CONFIG

    # --- URL rules ---
    def url_ok(u):
        if not isinstance(u, str):
            return False
        p = urlsplit(u)
        return p.scheme in c["allowed_schemes"] and "." in p.netloc

    df["is_valid_url"] = df["url_norm"].map(url_ok)

    # --- Date rules: parseable, not in the future, not before min_date ---
    now = pd.Timestamp.now(tz="UTC")
    min_date = pd.Timestamp(c["min_date"], tz="UTC")
    df["is_valid_date"] = df["date_utc"].notna() & (df["date_utc"] <= now) \
        & (df["date_utc"] >= min_date)

    # --- Text rules ---
    title_len = df["title"].fillna("").str.len()
    body_len = df["body"].fillna("").str.len()
    df["is_valid_title"] = title_len >= c["min_title_chars"]
    df["is_valid_body"] = body_len >= c["min_body_chars"]

    # --- Reason codes. One row can have several reasons. ---
    reasons = pd.DataFrame(index=df.index)
    reasons["missing_url"] = df["url"].isna()
    reasons["invalid_url"] = ~df["is_valid_url"] & df["url"].notna()
    reasons["missing_date"] = df["published_date"].isna()
    reasons["invalid_date"] = ~df["is_valid_date"] & df["published_date"].notna()
    reasons["missing_title"] = df["title"].isna()
    reasons["short_title"] = ~df["is_valid_title"] & df["title"].notna()
    reasons["empty_body"] = df["body"].isna()
    reasons["short_body"] = ~df["is_valid_body"] & df["body"].notna()
    # combined rule example: a row with no usable URL AND no body is useless
    reasons["invalid_url_and_no_body"] = ~df["is_valid_url"] & ~df["is_valid_body"]

    df["flags"] = reasons.apply(lambda r: ",".join(r.index[r.values]), axis=1)

    reject = reasons[[r for r in c["reject_rules"] if r in reasons.columns]].any(axis=1)
    df["is_rejected"] = reject
    df["reject_reason"] = reasons[[r for r in c["reject_rules"]
                                   if r in reasons.columns]].apply(
        lambda r: ",".join(r.index[r.values]), axis=1)

    counts = reasons.sum().sort_values(ascending=False)
    for rule, n in counts.items():
        log.info("  rule %-26s %6d rows (%.1f%%)", rule, n, 100 * n / max(len(df), 1))
    log.info("  -> marked REJECT: %d rows", int(reject.sum()))

    tracker.record("4_validation", len(df),
                   f"{int(reject.sum())} marked for rejection, rest only flagged")
    df.attrs["validation_counts"] = {k: int(v) for k, v in counts.items()}
    return df


# =====================================================================
# STAGE 6 helper is needed by Stage 5 (we dedup on lightly-cleaned text),
# so text cleaning functions are defined first.
# =====================================================================
_ws_re = re.compile(r"[ \t\u00a0\u200b\u2009\u202f]+")
_blank_lines_re = re.compile(r"\n{3,}")


def strip_html(text: str) -> str:
    """Remove tags with a real HTML parser (safer than regex), keep paragraph breaks."""
    if "<" not in text and "&" not in text:
        return text
    soup = BeautifulSoup(text, "lxml")
    for tag in soup(["script", "style", "noscript", "iframe", "svg", "form",
                     "nav", "footer", "header", "aside"]):
        tag.decompose()                      # remove code/menus entirely
    for br in soup.find_all("br"):
        br.replace_with("\n")
    for p in soup.find_all(["p", "div", "li", "h1", "h2", "h3", "h4", "tr"]):
        p.append("\n")                       # keep paragraph structure
    return soup.get_text()


def clean_text_basic(text) -> str | None:
    """HTML -> text, decode entities, fix unicode, normalize whitespace."""
    if text is None or (not isinstance(text, str) and pd.isna(text)):
        return None
    text = str(text)
    text = fix_mojibake(text)                # must run BEFORE NFKC (NFKC would damage it)
    text = strip_html(text)
    text = html.unescape(text)               # &amp; -> &   &quot; -> "
    text = html.unescape(text)               # twice: handles '&amp;amp;'
    text = unicodedata.normalize("NFKC", text)   # unify look-alike characters
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[\u0000-\u0008\u000b\u000c\u000e-\u001f\u200b-\u200d\ufeff]", "", text)
    text = _ws_re.sub(" ", text)
    text = "\n".join(line.strip() for line in text.split("\n"))
    text = _blank_lines_re.sub("\n\n", text)
    return text.strip()


def fix_mojibake(text: str) -> str:
    """Repair text like 'Itâ€™s' -> 'It's' (UTF-8 wrongly read as Latin-1)."""
    if not any(m in text for m in ("â€", "Ã©", "Ã¨", "Ã¢", "Â ", "Ã¼")):
        return text
    try:
        fixed = text.encode("cp1252", errors="strict").decode("utf-8", errors="strict")
        return fixed
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


# =====================================================================
# STAGE 5 - DEDUPLICATION
# =====================================================================
_MERSENNE = (1 << 61) - 1


def _shingles(words: list, k: int) -> np.ndarray:
    """Overlapping groups of k words, each turned into a 32-bit number."""
    if len(words) < k:
        grams = [" ".join(words)]
    else:
        grams = [" ".join(words[i:i + k]) for i in range(len(words) - k + 1)]
    return np.unique(np.fromiter((zlib.crc32(g.encode("utf-8")) for g in grams),
                                 dtype=np.uint64, count=len(grams)))


def _minhash_signature(shingle_hashes: np.ndarray, a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Compact fingerprint (64 numbers). Two similar texts have signatures
    that agree in many positions. Agreement rate ~= Jaccard similarity."""
    # (a*x + b) mod prime, one row per permutation, take the min per row
    x = shingle_hashes[None, :]
    vals = (a[:, None] * x + b[:, None]) % np.uint64(_MERSENNE)
    return vals.min(axis=1)


class UnionFind:
    """Tiny helper to merge 'A~B' and 'B~C' into one group {A,B,C}."""

    def __init__(self, n):
        self.parent = list(range(n))

    def find(self, x):
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def stage5_deduplicate(df: pd.DataFrame, tracker: StageTracker) -> pd.DataFrame:
    log.info("=== STAGE 5: DEDUPLICATION ===")
    c = CONFIG
    rng = np.random.default_rng(c["random_seed"])

    # Light text clean just for comparing (the real cleaned text comes in Stage 6)
    # Cleaned once here and re-used in Stage 6 (HTML parsing is the slowest part).
    df["_body_basic"] = df["body"].map(lambda t: clean_text_basic(t) if isinstance(t, str) else None)
    comp_text = df["_body_basic"].fillna("")
    norm_text = comp_text.str.lower().str.replace(r"[^\w\s]", "", regex=True) \
        .str.replace(r"\s+", " ", regex=True).str.strip()

    # --- 5a. Exact duplicates by normalized URL ---
    url_dup = df["url_norm"].notna() & df.duplicated("url_norm", keep=False)

    # --- 5b. Exact duplicates by content hash (same text, different URL) ---
    df["content_hash"] = norm_text.map(
        lambda t: hashlib.md5(t.encode("utf-8")).hexdigest() if t else None)
    hash_dup = df["content_hash"].notna() & df.duplicated("content_hash", keep=False)
    log.info("  exact duplicates by URL: %d rows | by content hash: %d rows",
             int(url_dup.sum()), int(hash_dup.sum()))

    uf = UnionFind(len(df))
    # connect rows sharing the same URL or the same content
    for key in ("url_norm", "content_hash"):
        first_seen = {}
        for i, v in enumerate(df[key].tolist()):
            if v is None or (isinstance(v, float) and np.isnan(v)) or v is pd.NA:
                continue
            if v in first_seen:
                uf.union(first_seen[v], i)
            else:
                first_seen[v] = i

    # --- 5c. Near duplicates: shingles -> MinHash -> LSH buckets -> verify ---
    n_perm, bands = c["minhash_permutations"], c["lsh_bands"]
    rows_per_band = n_perm // bands
    a = rng.integers(1, _MERSENNE, size=n_perm, dtype=np.uint64)
    b = rng.integers(0, _MERSENNE, size=n_perm, dtype=np.uint64)

    words_list = norm_text.str.split().tolist()
    signatures = {}
    for i, words in enumerate(words_list):
        if len(words) >= c["dedup_min_words"]:
            sh = _shingles(words, c["dedup_shingle_size"])
            signatures[i] = _minhash_signature(sh, a, b)

    buckets = {}
    for i, sig in signatures.items():
        for band in range(bands):
            chunk = sig[band * rows_per_band:(band + 1) * rows_per_band]
            buckets.setdefault((band, chunk.tobytes()), []).append(i)

    near_pairs, checked = 0, set()
    for members in buckets.values():
        if len(members) < 2 or len(members) > 200:   # skip giant junk buckets
            continue
        for x in range(len(members)):
            for y in range(x + 1, len(members)):
                i, j = members[x], members[y]
                if (i, j) in checked:
                    continue
                checked.add((i, j))
                est_jaccard = float(np.mean(signatures[i] == signatures[j]))
                if est_jaccard >= c["near_dup_threshold"]:
                    uf.union(i, j)
                    near_pairs += 1
    log.info("  near-duplicate pairs found (>= %.2f): %d", c["near_dup_threshold"], near_pairs)

    # --- 5d. Build groups and choose which copy to KEEP. Others are only marked. ---
    root = np.array([uf.find(i) for i in range(len(df))])
    df["duplicate_group_id"] = root
    group_size = pd.Series(root).map(pd.Series(root).value_counts()).to_numpy()
    df["duplicate_group_size"] = group_size

    body_len = df["body"].fillna("").str.len().to_numpy()
    secs = to_seconds(df["date_utc"]).fillna(np.inf).to_numpy()
    rejected = df["is_rejected"].to_numpy()
    keep_idx = set()
    for g, idxs in pd.Series(range(len(df))).groupby(root):
        idxs = list(idxs)
        if len(idxs) == 1:
            keep_idx.add(idxs[0])
            continue
        # prefer a copy that is not rejected, so a good copy survives if one exists
        pool = [k for k in idxs if not rejected[k]] or idxs
        if c["dedup_keep"] == "longest":
            keep_idx.add(max(pool, key=lambda k: body_len[k]))
        elif c["dedup_keep"] == "earliest":
            keep_idx.add(min(pool, key=lambda k: secs[k]))
        else:
            keep_idx.add(pool[0])

    df["is_duplicate"] = [i not in keep_idx for i in range(len(df))]
    n_dup = int(df["is_duplicate"].sum())
    log.info("  duplicate groups: %d | rows marked duplicate (not kept): %d",
             int((pd.Series(root).value_counts() > 1).sum()), n_dup)
    tracker.record("5_deduplication", len(df),
                   f"{n_dup} rows marked as duplicates (kept 1 per group)")
    return df


# =====================================================================
# STAGE 6 - CLEANING
# =====================================================================
def find_boilerplate_lines(df: pd.DataFrame) -> dict:
    """Lines that appear in many articles of the SAME source are menus/footers,
    not real content. Returns {source: set_of_lines}."""
    c = CONFIG
    result = {}
    for src, grp in df.groupby("source_std"):
        if len(grp) < c["boilerplate_min_articles"]:
            continue
        counter = {}
        for text in grp["body_clean_tmp"]:
            if not isinstance(text, str):
                continue
            for line in set(l.strip() for l in text.split("\n") if 3 < len(l.strip()) < 200):
                counter[line] = counter.get(line, 0) + 1
        cutoff = c["boilerplate_line_share"] * len(grp)
        result[src] = {l for l, n in counter.items() if n >= cutoff}
    return result


def remove_boilerplate(text, repeated_lines: set):
    """Drop lines that are repeated across the source or contain known junk phrases."""
    if not isinstance(text, str):
        return text
    phrases = CONFIG["boilerplate_phrases"]
    kept = []
    for line in text.split("\n"):
        low = line.strip().lower()
        short_junk = len(low) < 120 and any(p in low for p in phrases)
        if line.strip() in repeated_lines or short_junk:
            continue
        kept.append(line)
    return _blank_lines_re.sub("\n\n", "\n".join(kept)).strip()


def stage6_clean(df: pd.DataFrame, tracker: StageTracker) -> pd.DataFrame:
    log.info("=== STAGE 6: CLEANING TEXT ===")
    # Raw columns are untouched. We write NEW columns: title_clean, body_clean.
    df["title_clean"] = df["title"].map(clean_text_basic).map(
        lambda t: re.sub(r"\s+", " ", t) if isinstance(t, str) else t)
    if "_body_basic" in df.columns:          # re-use the text cleaned in Stage 5
        df["body_clean_tmp"] = df["_body_basic"]
    else:
        df["body_clean_tmp"] = df["body"].map(clean_text_basic)

    if CONFIG["remove_boilerplate"]:
        repeated = find_boilerplate_lines(df)
        log.info("  boilerplate lines learned for %d sources", len(repeated))
        df["body_clean"] = [
            remove_boilerplate(t, repeated.get(s, set()))
            for t, s in zip(df["body_clean_tmp"], df["source_std"])
        ]
    else:
        df["body_clean"] = df["body_clean_tmp"]

    before = df["body"].fillna("").str.len()
    after = df["body_clean"].fillna("").str.len()
    df["chars_removed"] = before - after
    df["body_chars"] = after
    df["body_words"] = df["body_clean"].fillna("").str.split().str.len()
    df.drop(columns=["body_clean_tmp", "_body_basic"], inplace=True, errors="ignore")

    log.info("  avg chars removed per article: %.0f", df["chars_removed"].mean())
    tracker.record("6_cleaning", len(df), "new columns title_clean, body_clean")
    return df


# =====================================================================
# STAGE 7 - QUALITY ASSESSMENT
# =====================================================================
def stage7_quality(df: pd.DataFrame, tracker: StageTracker) -> pd.DataFrame:
    log.info("=== STAGE 7: QUALITY ASSESSMENT ===")
    c = CONFIG

    # 7a. Completeness: share of required fields that are present AND valid
    valid_map = {
        "url": df["is_valid_url"],
        "title": df["is_valid_title"],
        "body": df["is_valid_body"],
        "published_date": df["is_valid_date"],
    }
    req = [f for f in c["required_fields"] if f in valid_map]
    df["completeness"] = pd.concat([valid_map[f] for f in req], axis=1).mean(axis=1)

    # 7b. Questionable-row signals
    text = df["body_clean"].fillna("")
    q = pd.DataFrame(index=df.index)

    letters = text.str.count(r"[^\W\d_]")
    q["high_non_letter_ratio"] = (1 - letters / text.str.len().clip(lower=1)) \
        > c["max_non_letter_ratio"]

    def repeated_ratio(t):
        lines = [l for l in t.split("\n") if l.strip()]
        return 0.0 if len(lines) < 4 else 1 - len(set(lines)) / len(lines)

    q["many_repeated_lines"] = text.map(repeated_ratio) > c["max_repeated_line_ratio"]

    # Day 43 IQR rule on text length (only among rows that have a body)
    lens = df.loc[df["body_chars"] > 0, "body_chars"]
    q1, q3 = lens.quantile(.25), lens.quantile(.75)
    iqr = q3 - q1
    low, high = q1 - c["iqr_multiplier"] * iqr, q3 + c["iqr_multiplier"] * iqr
    q["length_outlier_long"] = df["body_chars"] > high
    q["length_outlier_short"] = (df["body_chars"] < max(low, 0)) & (df["body_chars"] > 0)
    log.info("  length IQR bounds: low=%.0f high=%.0f", low, high)

    # Date far from the rest of the same source (median +/- 5 IQR of its dates)
    ts = to_seconds(df["date_utc"])
    q["date_outlier_for_source"] = False
    for src, idx in df.groupby("source_std").groups.items():
        s = ts.loc[idx].dropna()
        if len(s) >= 20:
            d = s.quantile(.75) - s.quantile(.25)
            if d > 0:
                bad = (ts.loc[idx] < s.median() - 5 * d) | (ts.loc[idx] > s.median() + 5 * d)
                q.loc[idx, "date_outlier_for_source"] = bad.fillna(False).to_numpy()

    q["is_duplicate"] = df["is_duplicate"]
    df["quality_flags"] = q.apply(lambda r: ",".join(r.index[r.values]), axis=1)

    # 7c. One score between 0 and 1: completeness minus penalties
    penalties = (q[["high_non_letter_ratio", "many_repeated_lines",
                    "length_outlier_long", "length_outlier_short",
                    "date_outlier_for_source"]].sum(axis=1) * 0.15)
    df["quality_score"] = (df["completeness"] - penalties).clip(0, 1).round(3)
    df["quality_flag"] = np.select(
        [df["quality_score"] >= c["quality_good"], df["quality_score"] >= c["quality_review"]],
        ["good", "review"], default="poor")

    log.info("  quality flags: %s", df["quality_flag"].value_counts().to_dict())
    tracker.record("7_quality", len(df), "completeness + quality_score added")
    return df


# =====================================================================
# STAGE 8 - EXPORT + REPORT
# =====================================================================
def save_table(df: pd.DataFrame, folder: str, name: str) -> list:
    """Parquet (fast, small, keeps types) with CSV as a human-readable copy."""
    import os
    out = Path(folder)
    out.mkdir(parents=True, exist_ok=True)
    written = []
    # Write to a temp file first, then atomically replace the destination.
    # Falls back to a timestamped name if the target is locked by the IDE or OneDrive.
    csv_path = out / f"{name}.csv"
    tmp_path = out / f"{name}.tmp.csv"
    df.to_csv(tmp_path, index=False, encoding="utf-8")
    try:
        os.replace(str(tmp_path), str(csv_path))   # atomic on Windows
        written.append(f"{name}.csv")
    except PermissionError:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        fallback = out / f"{name}_{ts}.csv"
        tmp_path.rename(fallback)
        log.warning("Could not replace %s (file locked). Saved as %s instead.",
                    csv_path.name, fallback.name)
        written.append(fallback.name)
    try:
        df.to_parquet(out / f"{name}.parquet", index=False)
        written.append(f"{name}.parquet")
    except Exception as e:
        log.warning("Parquet failed (%s). Saving CSV only.", e)
    return written


def stage8_export(df: pd.DataFrame, tracker: StageTracker, profile: dict,
                  ingest_info: dict, args) -> None:
    log.info("=== STAGE 8: EXPORT ===")
    c = CONFIG

    rejected = df[df["is_rejected"]]
    dupes = df[(~df["is_rejected"]) & df["is_duplicate"]]
    if c["drop_duplicates_from_clean"]:
        clean = df[(~df["is_rejected"]) & (~df["is_duplicate"])]
    else:
        clean = df[~df["is_rejected"]]
    flagged = clean[clean["quality_flag"] != "good"]

    # Clean file: final columns first, original raw_* columns kept at the end (P1)
    front = ["row_id", "url_norm", "domain", "source_std", "title_clean", "body_clean",
             "date_iso", "author", "body_chars", "body_words", "completeness",
             "quality_score", "quality_flag", "quality_flags", "flags",
             "duplicate_group_id", "duplicate_group_size", "is_duplicate"]
    ordered = [col for col in front if col in clean.columns]
    rest = [col for col in clean.columns if col not in ordered]

    files = {}
    files["clean"] = save_table(clean[ordered + rest].drop(columns=["date_utc"],
                                                           errors="ignore"),
                                c["clean_dir"], "articles_clean")
    files["rejected"] = save_table(rejected.drop(columns=["date_utc"], errors="ignore"),
                                   c["rejected_dir"], "articles_rejected")
    files["flagged_for_review"] = save_table(
        flagged[["row_id", "url_norm", "title_clean", "quality_score",
                 "quality_flag", "quality_flags", "flags"]],
        c["rejected_dir"], "articles_flagged_for_review")
    files["duplicates"] = save_table(
        dupes[["row_id", "url_norm", "duplicate_group_id", "duplicate_group_size"]],
        c["rejected_dir"], "articles_duplicates_map")

    tracker.record("8_export_clean", len(clean), "final clean rows")
    tracker.record("8_export_rejected", len(rejected), "rejected rows (with reason)")
    tracker.record("8_export_duplicates", len(dupes), "duplicate rows (not kept)")

    # Row accounting: every input row must be in exactly one bucket
    accounted = len(clean) + len(rejected) + len(dupes)
    assert accounted == len(df), f"Row accounting broken: {accounted} != {len(df)}"

    # ---- reproducible report ----
    per_source = (df.groupby("source_std")
                  .agg(articles=("row_id", "count"),
                       avg_quality=("quality_score", "mean"),
                       pct_poor=("quality_flag", lambda s: 100 * (s == "poor").mean()))
                  .sort_values("avg_quality").round(3))
    report = {
        "run_at_utc": datetime.now(timezone.utc).isoformat(),
        "command_args": vars(args),
        "config_used": CONFIG,
        "ingestion": ingest_info,
        "row_counts_by_stage": tracker.rows,
        "final_split": {"clean": len(clean), "rejected": len(rejected),
                        "duplicates": len(dupes),
                        "clean_flagged_for_review": len(flagged)},
        "validation_rule_counts": df.attrs.get("validation_counts", {}),
        "completeness_overall": round(float(df["completeness"].mean()), 4),
        "quality_distribution": df["quality_flag"].value_counts().to_dict(),
        "profile_before_cleaning": profile,
        "worst_sources": per_source.head(10).reset_index().to_dict("records"),
        "output_files": files,
    }
    Path(c["reports_dir"]).mkdir(parents=True, exist_ok=True)
    with open(Path(c["reports_dir"]) / "cleaning_report.json", "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    md = ["# Cleaning Report", f"_Run at {report['run_at_utc']}_", "",
          "## Where did every row go?", "| Stage | Rows | Note |", "|---|---|---|"]
    md += [f"| {r['stage']} | {r['rows']:,} | {r['note']} |" for r in tracker.rows]
    md += ["", "## Final split",
           f"- Clean: **{len(clean):,}**", f"- Rejected: **{len(rejected):,}**",
           f"- Duplicates (not kept): **{len(dupes):,}**",
           f"- Clean but flagged for review: **{len(flagged):,}**",
           f"- Overall completeness: **{report['completeness_overall']:.1%}**", "",
           "## Validation rule counts"]
    md += [f"- {k}: {v:,}" for k, v in report["validation_rule_counts"].items()]
    md += ["", "## Quality distribution"]
    md += [f"- {k}: {v:,}" for k, v in report["quality_distribution"].items()]
    md += ["", "## Lowest-quality sources", per_source.head(10).to_markdown()
           if _has_tabulate() else per_source.head(10).to_string()]
    (Path(c["reports_dir"]) / "cleaning_report.md").write_text("\n".join(md), encoding="utf-8")
    log.info("Report saved to %s", c["reports_dir"])


def _has_tabulate() -> bool:
    try:
        import tabulate  # noqa: F401
        return True
    except ImportError:
        return False


# =====================================================================
# MAIN - runs the 8 stages in order
# =====================================================================
def main():
    parser = argparse.ArgumentParser(description="Article cleaning pipeline")
    parser.add_argument("--input", required=True, help="file or folder of scraped data")
    parser.add_argument("--sample", type=int, default=None,
                        help="use only N random rows (try 1000 first!)")
    parser.add_argument("--config", default=None,
                        help="optional JSON file that overrides CONFIG values")
    args = parser.parse_args()

    if args.config:
        CONFIG.update(json.loads(Path(args.config).read_text()))
    setup_logging(CONFIG["logs_dir"])

    t0 = time.time()
    tracker = StageTracker()
    df, ingest_info = stage1_ingest(args.input, args.sample, tracker)
    profile = stage2_profile(df)
    df = stage3_standardize(df, tracker)
    df = stage4_validate(df, tracker)
    df = stage5_deduplicate(df, tracker)
    df = stage6_clean(df, tracker)
    df = stage7_quality(df, tracker)
    stage8_export(df, tracker, profile, ingest_info, args)
    log.info("DONE in %.1f seconds", time.time() - t0)


if __name__ == "__main__":
    main()
