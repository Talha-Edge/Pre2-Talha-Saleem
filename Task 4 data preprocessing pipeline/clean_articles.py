"""
Data Preprocessing Pipeline for Scraped News Articles
Based strictly on the LimeX Data Preprocessing Lecture Curriculum.

Pipeline Stages:
1. Setup and Ingestion (dynamic CSV resolution with fallback)
2. Raw Data Profiling
3. Schema Standardization & Canonical Coalescing (news_url, title, published_time, author, content)
4. Missing Value & Whitespace Normalization
5. HTML Sanitization & Typography Normalization
6. URL Validation & Domain Extraction
7. Robust UTC Timestamp Parsing & Temporal Decomposition
8. Text Length & Extraction-Quality Feature Engineering
9. Multi-tier Duplicate Detection (Exact, URL, and Content Signature)
10. Row-Level Quality Auditing (Critical, Review, Info Issues)
11. Dataset-Level Completeness & Metrics Reporting
12. NLP Partitioning (Clean, Review, Rejected) with Reconciliation
13. Export Artifacts (CSV & JSON Summary)
"""

import sys
import os
import re
import html
import json
import argparse
import unicodedata
from pathlib import Path
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from html.parser import HTMLParser

import numpy as np
import pandas as pd


# ==============================================================================
# 1. SETUP & CONFIGURATION
# ==============================================================================

CANDIDATE_PATHS = [
    Path(r"C:\Users\talha\OneDrive\Desktop\Limox\day 4\output_bulk\final\articles_bulk_new.csv"),
    Path(r"C:\Users\talha\OneDrive\Desktop\Limox\day 4\output_bulk\final\articles_bulk.csv"),
    Path("../day 4/output_bulk/final/articles_bulk_new.csv"),
    Path("../day 4/output_bulk/final/articles_bulk.csv"),
    Path("scraped_news.csv"),
]

ALIASES = {
    "news_url": ["url", "article_url", "shared_url", "source_url", "link", "news_link"],
    "title": ["headline", "article_title", "news_title"],
    "published_time": ["time", "date", "published", "published_date", "publication_time", "datetime", "timestamp"],
    "author": ["writer", "byline", "article_author", "authors"],
    "content": ["headlines", "text", "article_content", "body", "article_text", "full_text", "news_content"],
}

REQUIRED_FIELDS = ["news_url", "title", "published_time", "author", "content"]
MISSING_TOKENS = {"", "n/a", "na", "null", "none", "nan", "-", "--"}
INVISIBLE_CHARS = dict.fromkeys(map(ord, "\u200b\u200c\u200d\u2060\ufeff\u00ad"), None)
TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content", "fbclid", "gclid"}

ERROR_PATTERNS = [
    r"access denied", r"enable javascript", r"captcha", r"robot check",
    r"page not found", r"403 forbidden", r"subscribe to continue"
]
ERROR_REGEX = re.compile("|".join(ERROR_PATTERNS), flags=re.I)

MIN_TITLE_CHARS = 8
MIN_CONTENT_CHARS = 200


# ==============================================================================
# 2. HELPER FUNCTIONS
# ==============================================================================

class TextExtractor(HTMLParser):
    SKIP_TAGS = {"script", "style", "noscript"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.parts = []
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in self.SKIP_TAGS:
            self._skip_depth += 1

    def handle_endtag(self, tag):
        if tag in self.SKIP_TAGS and self._skip_depth:
            self._skip_depth -= 1

    def handle_data(self, data):
        if self._skip_depth == 0 and data and data.strip():
            self.parts.append(data.strip())


def strip_html(value):
    if pd.isna(value):
        return pd.NA
    text = str(value)
    parser = TextExtractor()
    try:
        parser.feed(text)
        parser.close()
        extracted = " ".join(parser.parts)
    except Exception:
        extracted = re.sub(r"<[^>]+>", " ", text)
    extracted = html.unescape(extracted)
    extracted = unicodedata.normalize("NFKC", extracted)
    extracted = re.sub(r"\s+", " ", extracted).strip()
    return extracted if extracted else pd.NA


def normalize_typography(value):
    if pd.isna(value):
        return pd.NA
    value = str(value).translate(INVISIBLE_CHARS)
    value = unicodedata.normalize("NFKC", value)
    value = value.replace("“", '"').replace("”", '"').replace("‘", "'").replace("’", "'")
    value = re.sub(r"\s+", " ", value).strip()
    return value if value else pd.NA


def normalize_url(value, remove_tracking=False):
    if pd.isna(value):
        return pd.NA
    value = str(value).strip()
    if not value or not re.match(r"^https?://", value, flags=re.I) or re.search(r"\s", value):
        return pd.NA
    try:
        p = urlparse(value)
        if not p.hostname:
            return pd.NA
        scheme = p.scheme.lower()
        hostname = p.hostname.lower()
        netloc = hostname
        if p.port and not ((scheme == "http" and p.port == 80) or (scheme == "https" and p.port == 443)):
            netloc = f"{hostname}:{p.port}"
        path = re.sub(r"/{2,}", "/", p.path or "/")
        query = p.query
        if remove_tracking and query:
            pairs = [(k, v) for k, v in parse_qsl(query, keep_blank_values=True)
                     if k.lower() not in TRACKING_PARAMS]
            query = urlencode(pairs, doseq=True)
        return urlunparse((scheme, netloc, path, "", query, ""))
    except (ValueError, TypeError):
        return pd.NA


def parse_timestamps(series):
    try:
        return pd.to_datetime(series, errors="coerce", utc=True, format="mixed")
    except (TypeError, ValueError):
        return pd.to_datetime(series, errors="coerce", utc=True)


def safe_len(value):
    return len(value) if isinstance(value, str) else 0


def word_count(value):
    return len(re.findall(r"\b[\w'-]+\b", value, flags=re.UNICODE)) if isinstance(value, str) else 0


def text_signature(title, content):
    t = title.casefold().strip() if isinstance(title, str) else ""
    c = content.casefold().strip() if isinstance(content, str) else ""
    return (t, c)


def standardize_column_name(name):
    name = unicodedata.normalize("NFKC", str(name)).strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def is_present(series):
    series = series.astype(object)
    return series.notna() & series.map(lambda v: not (isinstance(v, str) and v.strip() == ""))


def coalesce_columns(frame, canonical, aliases):
    names = [canonical] + aliases
    positions = [i for i, c in enumerate(frame.columns) if c in names]
    if not positions:
        return frame, None
    positions.sort(key=lambda i: names.index(frame.columns[i]))
    block = frame.iloc[:, positions].astype(object)
    merged = block.iloc[:, 0]
    conflicts = 0
    for k in range(1, block.shape[1]):
        other = block.iloc[:, k]
        both = is_present(merged) & is_present(other)
        conflicts += int((both & (merged.astype(str) != other.astype(str))).sum())
        merged = merged.where(is_present(merged), other)
    rest = frame.iloc[:, [i for i in range(frame.shape[1]) if i not in positions]]
    out = pd.concat([pd.DataFrame({canonical: merged}), rest], axis=1)
    return out, {"merged_from": [frame.columns[i] for i in positions], "conflicts": conflicts}


# ==============================================================================
# 3. PIPELINE EXECUTION
# ==============================================================================

def run_pipeline(input_path=None, output_dir=None, sample_size=None):
    # Resolve input path
    if input_path and Path(input_path).exists():
        file_path = Path(input_path)
    else:
        file_path = next((p for p in CANDIDATE_PATHS if p.exists()), None)
        if file_path is None:
            raise FileNotFoundError("No input file found among candidate paths.")

    output_dir = Path(output_dir) if output_dir else Path("preprocessed_output")
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[1/13] Loading dataset from: {file_path}")
    raw_df = pd.read_csv(file_path, encoding="utf-8", low_memory=False, nrows=sample_size)
    print(f"       Loaded {len(raw_df):,} rows, {len(raw_df.columns)} columns.")

    print("\n[2/13] Standardizing column names and canonical coalescing...")
    df = raw_df.copy(deep=True)
    df.columns = [standardize_column_name(c) for c in df.columns]

    for canonical, aliases in ALIASES.items():
        df, _ = coalesce_columns(df, canonical, aliases)

    for col in REQUIRED_FIELDS:
        if col not in df.columns:
            df[col] = pd.NA

    print("\n[3/13] Normalizing missing tokens and whitespace...")
    for col in REQUIRED_FIELDS:
        df[f"{col}_raw"] = df[col]

    def normalize_missing(val):
        if pd.isna(val):
            return pd.NA
        if isinstance(val, str):
            val = val.replace("\u00a0", " ").strip()
            if val.casefold() in MISSING_TOKENS:
                return pd.NA
        return val

    for col in REQUIRED_FIELDS:
        df[col] = df[col].map(normalize_missing)

    def normalize_ws(val):
        if pd.isna(val):
            return pd.NA
        t = str(val).replace("\u00a0", " ")
        t = re.sub(r"[\t\r\f\v]+", " ", t)
        t = re.sub(r" *\n *", "\n", t)
        t = re.sub(r" {2,}", " ", t)
        return t.strip()

    for col in ["title", "author", "content"]:
        df[col] = df[col].map(normalize_ws)

    print("\n[4/13] Stripping HTML and normalizing typography...")
    for col in ["title", "author", "content"]:
        df[f"{col}_clean"] = df[col].map(strip_html).map(normalize_typography)

    print("\n[5/13] Validating and normalizing URLs...")
    df["news_url_clean"] = df["news_url"].map(lambda x: normalize_url(x, remove_tracking=False))
    df["url_valid"] = df["news_url_clean"].notna()
    df["domain"] = df["news_url_clean"].map(lambda x: urlparse(x).hostname if pd.notna(x) else pd.NA)

    print("\n[6/13] Parsing timestamps to UTC...")
    df["published_at_utc"] = parse_timestamps(df["published_time"])
    df["published_date_utc"] = df["published_at_utc"].dt.date
    df["published_year"] = df["published_at_utc"].dt.year
    df["published_month"] = df["published_at_utc"].dt.month
    df["published_weekday"] = df["published_at_utc"].dt.day_name()

    print("\n[7/13] Extracting text length and error features...")
    df["title_char_count"] = df["title_clean"].map(safe_len)
    df["title_word_count"] = df["title_clean"].map(word_count)
    df["content_char_count"] = df["content_clean"].map(safe_len)
    df["content_word_count"] = df["content_clean"].map(word_count)
    df["possible_block_or_paywall"] = df["content_clean"].map(
        lambda x: bool(ERROR_REGEX.search(x)) if isinstance(x, str) else False
    )
    df["title_too_short"] = df["title_char_count"] < MIN_TITLE_CHARS
    df["content_too_short"] = df["content_char_count"] < MIN_CONTENT_CHARS

    print("\n[8/13] Detecting exact and signature duplicates...")
    dup_cols = ["news_url_clean", "title_clean", "published_at_utc", "author_clean", "content_clean"]
    df["is_exact_duplicate"] = df.duplicated(subset=dup_cols, keep="first")
    df["is_duplicate_url"] = df["news_url_clean"].notna() & df.duplicated(subset=["news_url_clean"], keep="first")
    df["text_signature"] = [text_signature(t, c) for t, c in zip(df["title_clean"], df["content_clean"])]
    df["is_duplicate_text"] = df["text_signature"].duplicated(keep="first") & (
        df["title_clean"].notna() | df["content_clean"].notna()
    )

    print("\n[9/13] Building row-level quality audit report...")
    df["missing_url"] = df["news_url_clean"].isna()
    df["missing_title"] = df["title_clean"].isna()
    df["missing_time"] = df["published_at_utc"].isna()
    df["missing_author"] = df["author_clean"].isna()
    df["missing_content"] = df["content_clean"].isna()

    critical_reasons, review_reasons, info_reasons = [], [], []
    for _, row in df.iterrows():
        c, r, i = [], [], []
        if row["missing_url"]:
            c.append("missing_or_invalid_url")
        if row["missing_title"]:
            c.append("missing_title")
        if row["missing_content"]:
            c.append("missing_content")
        elif row["content_too_short"]:
            r.append("short_content")
        if row["title_too_short"] and not row["missing_title"]:
            r.append("short_title")
        if row["missing_time"]:
            r.append("missing_or_invalid_publication_time")
        if row["possible_block_or_paywall"]:
            r.append("possible_block_or_paywall")
        if row["missing_author"]:
            i.append("missing_author")
        critical_reasons.append(";".join(c))
        review_reasons.append(";".join(r))
        info_reasons.append(";".join(i))

    df["critical_issues"] = critical_reasons
    df["review_issues"] = review_reasons
    df["info_issues"] = info_reasons
    df["is_critical"] = df["critical_issues"].ne("")
    df["needs_review"] = df["review_issues"].ne("")

    print("\n[10/13] Calculating dataset-level completeness metrics...")
    quality_rows = []
    for col in ["news_url_clean", "title_clean", "published_at_utc", "author_clean", "content_clean"]:
        non_m = int(df[col].notna().sum())
        quality_rows.append({
            "field": col,
            "total_rows": len(df),
            "non_missing": non_m,
            "missing": int(df[col].isna().sum()),
            "completeness_pct": round(non_m / len(df) * 100, 2) if len(df) else 0.0,
            "distinct_non_missing": int(df[col].nunique(dropna=True))
        })
    quality_report = pd.DataFrame(quality_rows)

    summary = {
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "invalid_or_missing_urls": int(df["missing_url"].sum()),
        "missing_titles": int(df["missing_title"].sum()),
        "missing_publication_times": int(df["missing_time"].sum()),
        "missing_authors": int(df["missing_author"].sum()),
        "missing_content": int(df["missing_content"].sum()),
        "critical_rows": int(df["is_critical"].sum()),
        "rows_needing_review": int(df["needs_review"].sum()),
        "exact_duplicate_rows": int(df["is_exact_duplicate"].sum()),
        "duplicate_url_rows": int(df["is_duplicate_url"].sum()),
        "duplicate_text_rows": int(df["is_duplicate_text"].sum()),
    }

    print("\n[11/13] Partitioning into clean, review, and rejected datasets...")
    df = df.sort_values(["news_url_clean"], na_position="last", kind="stable").reset_index(drop=True)

    df["reject_for_full_text_nlp"] = (
        df["news_url_clean"].isna()
        | df["title_clean"].isna()
        | df["content_clean"].isna()
        | df["is_exact_duplicate"]
    )

    reject_checks = {
        "missing_or_invalid_url": df["news_url_clean"].isna(),
        "missing_title": df["title_clean"].isna(),
        "missing_content": df["content_clean"].isna(),
        "exact_duplicate": df["is_exact_duplicate"],
    }
    df["reject_reason"] = [
        ";".join(name for name, flag in zip(reject_checks, flags) if flag)
        for flags in zip(*reject_checks.values())
    ]

    rejected_df = df[df["reject_for_full_text_nlp"]].copy()
    clean_df = df[~df["reject_for_full_text_nlp"]].copy()
    review_df = df[df["needs_review"]].copy()

    assert len(clean_df) + len(rejected_df) == len(df) == len(raw_df), "Reconciliation assertion failed!"

    summary.update({
        "clean_rows": int(len(clean_df)),
        "review_rows": int(len(review_df)),
        "rejected_rows": int(len(rejected_df)),
    })

    print(f"       Clean Rows:    {len(clean_df):,}")
    print(f"       Review Rows:   {len(review_df):,}")
    print(f"       Rejected Rows: {len(rejected_df):,}")

    print("\n[12/13] Exporting preprocessed CSV and JSON artifacts...")
    clean_export = clean_df.drop(columns=["text_signature"], errors="ignore")
    review_export = review_df.drop(columns=["text_signature"], errors="ignore")
    rejected_export = rejected_df.drop(columns=["text_signature"], errors="ignore")

    clean_export.to_csv(output_dir / "news_clean.csv", index=False, encoding="utf-8-sig")
    review_export.to_csv(output_dir / "news_review.csv", index=False, encoding="utf-8-sig")
    rejected_export.to_csv(output_dir / "news_rejected.csv", index=False, encoding="utf-8-sig")
    quality_report.to_csv(output_dir / "news_quality_report.csv", index=False, encoding="utf-8-sig")

    with open(output_dir / "news_preprocessing_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False, default=str)

    print("\n[13/13] Generated Files in:", output_dir.resolve())
    for item in sorted(output_dir.iterdir()):
        print(f" - {item.name:32s} ({item.stat().st_size:,} bytes)")

    print("\nSUCCESS: Preprocessing pipeline completed successfully according to lecture standards!\n")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run LimeX News Preprocessing Pipeline")
    parser.add_argument("--input", default=None, help="Input CSV path")
    parser.add_argument("--output-dir", default="preprocessed_output", help="Output directory")
    parser.add_argument("--sample", type=int, default=None, help="Sample size (rows)")
    args = parser.parse_args()

    run_pipeline(input_path=args.input, output_dir=args.output_dir, sample_size=args.sample)
