"""
Data Validation & Quality Engineering Pipeline for Scraped News Articles
Based strictly on the LimeX Data Validation & Quality Assessment Lecture Curriculum.

Pipeline Stages:
1. Load dataset (clean dataset from Day 7 with candidate resolution)
2. Contract Definition (required/optional columns, thresholds, error patterns)
3. Schema Validation (duplicate columns, missing required/optional columns)
4. Field & Type Auditing (missing required fields, unexpected non-text datatypes)
5. URL & Hostname Integrity (HTTP/HTTPS syntax, domain synchronization)
6. Temporal Audit (UTC parse verification, chronology: pub <= scrape)
7. Text Quality & Access Block Auditing (min character lengths, CAPTCHA/paywall regex)
8. Duplicate & Conflicting Records (repeated URLs, repeated title/content signatures)
9. Completeness & Summary Metrics
10. Disposition Assignment (ACCEPT, WARNING, QUARANTINE with Fatal Rule gating)
11. Unit Test Verification
12. Artifact Serialization (CSV views & JSON summary)
"""

import sys
import os
import re
import json
import argparse
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import pandas as pd


# ==============================================================================
# 1. CONTRACT & CONFIGURATION
# ==============================================================================

CONTRACT = {
    "required_columns": ["news_url_clean", "title_clean", "content_clean"],
    "optional_columns": ["published_at_utc", "author_clean", "domain", "scraped_at_utc"],
    "min_title_chars_warning": 8,
    "min_content_chars_warning": 200,
    "known_error_patterns": [
        r"access denied", r"enable javascript", r"captcha",
        r"robot check", r"403 forbidden", r"page not found"
    ],
}

FATAL_RULES = {"FIELD_001", "URL_001"}

CANDIDATE_INPUTS = [
    Path(r"C:\Users\talha\OneDrive\Desktop\Limox\day 7\preprocessed_output\news_clean.csv"),
    Path("../day 7/preprocessed_output/news_clean.csv"),
    Path("../Task 4 data preprocessing pipeline/preprocessed_output/news_clean.csv"),
    Path("preprocessed_output/news_clean.csv"),
    Path("news_clean.csv"),
]


# ==============================================================================
# 2. VALIDATION HELPERS & UNIT TESTS
# ==============================================================================

def add_issue(issues, rule_id, severity, row_index, field, message):
    issues.append({
        "rule_id": rule_id,
        "severity": severity,
        "scope": "row" if row_index is not None else "dataset",
        "row_index": int(row_index) if row_index is not None else None,
        "field": field,
        "message": message
    })


def is_missing(value):
    if pd.isna(value):
        return True
    return isinstance(value, str) and not value.strip()


def is_valid_http_url(value):
    if not isinstance(value, str) or not value.strip():
        return False
    try:
        parsed = urlparse(value.strip())
        return parsed.scheme.lower() in {"http", "https"} and bool(parsed.hostname) and "." in parsed.hostname
    except Exception:
        return False


def run_unit_tests():
    assert is_valid_http_url("https://example.com/story/1"), "URL test 1 failed"
    assert is_valid_http_url("http://news.example.org/"), "URL test 2 failed"
    assert not is_valid_http_url("not-a-url"), "URL test 3 failed"
    assert not is_valid_http_url("ftp://example.com/file"), "URL test 4 failed"
    assert not is_valid_http_url(None), "URL test 5 failed"

    assert is_missing(None), "Missing test 1 failed"
    assert is_missing(np.nan), "Missing test 2 failed"
    assert is_missing("   "), "Missing test 3 failed"
    assert not is_missing("Article title"), "Missing test 4 failed"

    assert pd.notna(pd.to_datetime("2026-10-27T09:30:00Z", errors="coerce", utc=True)), "Timestamp test 1 failed"
    assert pd.isna(pd.to_datetime("yesterday maybe", errors="coerce", utc=True)), "Timestamp test 2 failed"
    print("All unit tests passed successfully!")


# ==============================================================================
# 3. SCHEMA VALIDATION
# ==============================================================================

def validate_schema(frame, contract):
    issues = []
    if frame.columns.duplicated().any():
        duplicates = frame.columns[frame.columns.duplicated()].tolist()
        issues.append({
            "rule_id": "SCHEMA_001", "severity": "ERROR", "scope": "dataset",
            "field": None, "row_index": None, "message": f"Duplicate column names: {duplicates}"
        })
    for col in contract["required_columns"]:
        if col not in frame.columns:
            issues.append({
                "rule_id": "SCHEMA_002", "severity": "ERROR", "scope": "dataset",
                "field": col, "row_index": None, "message": f"Missing required column: {col}"
            })
    for col in contract["optional_columns"]:
        if col not in frame.columns:
            issues.append({
                "rule_id": "SCHEMA_003", "severity": "INFO", "scope": "dataset",
                "field": col, "row_index": None, "message": f"Optional column not present: {col}"
            })
    return issues


# ==============================================================================
# 4. MAIN VALIDATION PIPELINE
# ==============================================================================

def run_validation(input_path=None, output_dir=None, sample_size=None):
    # Run self-tests first
    run_unit_tests()

    # Resolve input path
    if input_path and Path(input_path).exists():
        file_path = Path(input_path)
    else:
        file_path = next((p for p in CANDIDATE_INPUTS if p.exists()), None)
        if file_path is None:
            raise FileNotFoundError("Clean dataset not found among candidate inputs. Run Day 7 pipeline first.")

    output_dir = Path(output_dir) if output_dir else Path("validation_output")
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n[1/10] Loading clean dataset from: {file_path}")
    data = pd.read_csv(file_path, low_memory=False, nrows=sample_size)
    print(f"       Loaded {len(data):,} records with {len(data.columns)} columns.")

    # 1. Schema Validation
    print("\n[2/10] Validating schema against data contract...")
    issues = validate_schema(data, CONTRACT)
    schema_errors = sum(1 for i in issues if i["severity"] == "ERROR")
    if schema_errors > 0:
        print(f"WARNING: Found {schema_errors} schema errors.")

    df = data.copy(deep=True).reset_index(drop=False).rename(columns={"index": "source_row_index"})

    # 2. Required Fields & Data Types
    print("\n[3/10] Auditing required fields and data types...")
    for _, row in df.iterrows():
        idx = row["source_row_index"]
        for field in CONTRACT["required_columns"]:
            if field in df.columns and is_missing(row[field]):
                add_issue(issues, "FIELD_001", "ERROR", idx, field, f"Required field '{field}' is missing or blank.")

        for field in ["title_clean", "content_clean", "author_clean"]:
            if field in df.columns and not pd.isna(row[field]) and not isinstance(row[field], str):
                add_issue(issues, "TYPE_001", "WARNING", idx, field,
                          f"Expected text value, found {type(row[field]).__name__}.")

    # 3. URL and Domain Validation
    print("\n[4/10] Validating URLs and domain consistency...")
    for _, row in df.iterrows():
        idx = row["source_row_index"]
        url = row.get("news_url_clean", pd.NA)
        if not is_missing(url) and not is_valid_http_url(url):
            add_issue(issues, "URL_001", "ERROR", idx, "news_url_clean",
                      "URL is not a plausible HTTP(S) URL with a hostname.")
        elif not is_missing(url):
            hostname = urlparse(str(url)).hostname.lower()
            domain = row.get("domain", pd.NA)
            if not is_missing(domain) and str(domain).lower() != hostname:
                add_issue(issues, "URL_002", "WARNING", idx, "domain",
                          f"Stored domain '{domain}' does not match URL hostname '{hostname}'.")

    # 4. Timestamp Validation
    print("\n[5/10] Auditing timestamps and temporal chronology...")
    for col in ["published_at_utc", "scraped_at_utc"]:
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce", utc=True)
        df[col + "_parsed"] = parsed
        for pos, (_, row) in enumerate(df.iterrows()):
            idx = row["source_row_index"]
            val = row[col]
            if is_missing(val):
                if col == "scraped_at_utc":
                    add_issue(issues, "TIME_001", "INFO", idx, col,
                              "Scrape timestamp missing; run-level traceability is reduced.")
            elif pd.isna(parsed.iloc[pos]):
                add_issue(issues, "TIME_002", "WARNING", idx, col, "Timestamp is present but cannot be parsed.")

    if "published_at_utc_parsed" in df.columns and "scraped_at_utc_parsed" in df.columns:
        for _, row in df.iterrows():
            idx = row["source_row_index"]
            pub, scrape = row["published_at_utc_parsed"], row["scraped_at_utc_parsed"]
            if pd.notna(pub) and pd.notna(scrape) and pub > scrape:
                add_issue(issues, "TIME_003", "WARNING", idx, "published_at_utc",
                          "Publication time is later than scrape time; verify timezone/source metadata.")

    # 5. Content Quality & Extraction-Failure Checks
    print("\n[6/10] Checking content quality, minimum length, and error responses...")
    error_regex = re.compile("|".join(CONTRACT["known_error_patterns"]), flags=re.I)
    for _, row in df.iterrows():
        idx = row["source_row_index"]
        title = row.get("title_clean", pd.NA)
        content = row.get("content_clean", pd.NA)
        if isinstance(title, str) and title.strip() and len(title.strip()) < CONTRACT["min_title_chars_warning"]:
            add_issue(issues, "TEXT_001", "WARNING", idx, "title_clean",
                      f"Title is shorter than {CONTRACT['min_title_chars_warning']} characters.")
        if isinstance(content, str) and content.strip():
            if len(content.strip()) < CONTRACT["min_content_chars_warning"]:
                add_issue(issues, "TEXT_002", "WARNING", idx, "content_clean",
                          f"Content is shorter than {CONTRACT['min_content_chars_warning']} characters.")
            if error_regex.search(content):
                add_issue(issues, "TEXT_003", "WARNING", idx, "content_clean",
                          "Content may be a block, CAPTCHA, or access-error response.")

    # 6. Duplicate & Conflicting Record Checks
    print("\n[7/10] Scanning for duplicate URLs and content signatures...")
    if "news_url_clean" in df.columns:
        valid_url_mask = df["news_url_clean"].map(is_valid_http_url)
        repeated_url_mask = valid_url_mask & df["news_url_clean"].duplicated(keep=False)
        for _, row in df[repeated_url_mask].iterrows():
            add_issue(issues, "DUP_001", "WARNING", row["source_row_index"], "news_url_clean",
                      "URL occurs more than once; inspect duplicate versus article update.")

    if {"title_clean", "content_clean"}.issubset(df.columns):
        title_norm = df["title_clean"].fillna("").astype(str).str.casefold().str.strip()
        content_norm = df["content_clean"].fillna("").astype(str).str.casefold().str.strip()
        signatures = title_norm + "||" + content_norm
        mask = signatures.duplicated(keep=False) & ((title_norm != "") | (content_norm != ""))
        for _, row in df[mask].iterrows():
            add_issue(issues, "DUP_002", "WARNING", row["source_row_index"], "title_clean/content_clean",
                      "Title/body signature repeats; inspect before deduplication.")

    issues_df = pd.DataFrame(issues, columns=["rule_id", "severity", "scope", "row_index", "field", "message"])
    print(f"       Total validation issues recorded: {len(issues_df):,}")

    # 7. Quality Metrics & Completeness
    print("\n[8/10] Calculating dataset-level quality metrics...")
    completeness = pd.DataFrame({
        "column": df.columns,
        "non_missing": [int(df[c].notna().sum()) for c in df.columns],
        "missing": [int(df[c].isna().sum()) for c in df.columns],
        "completeness_pct": [round(df[c].notna().mean() * 100, 2) if len(df) else np.nan for c in df.columns]
    })

    # 8. Assign Dispositions (ACCEPT, WARNING, QUARANTINE)
    print("\n[9/10] Assigning row dispositions (ACCEPT, WARNING, QUARANTINE)...")
    row_issue_map = {}
    for _, issue in issues_df.iterrows():
        if pd.notna(issue["row_index"]):
            row_issue_map.setdefault(int(issue["row_index"]), []).append(issue["rule_id"])

    df["validation_issue_count"] = df["source_row_index"].map(lambda x: len(row_issue_map.get(int(x), [])))
    df["validation_issue_codes"] = df["source_row_index"].map(lambda x: ";".join(row_issue_map.get(int(x), [])))

    def disposition(row_id):
        rules = row_issue_map.get(int(row_id), [])
        if any(rule in FATAL_RULES for rule in rules):
            return "QUARANTINE"
        if rules:
            return "WARNING"
        return "ACCEPT"

    df["validation_disposition"] = df["source_row_index"].map(disposition)
    accepted_df = df[df["validation_disposition"] == "ACCEPT"].copy()
    warning_df = df[df["validation_disposition"] == "WARNING"].copy()
    quarantine_df = df[df["validation_disposition"] == "QUARANTINE"].copy()

    print(f"       Accepted:   {len(accepted_df):,}")
    print(f"       Warning:    {len(warning_df):,}")
    print(f"       Quarantine: {len(quarantine_df):,}")

    # Build Summary
    if issues_df.empty:
        by_severity, by_rule, affected_rows = {}, {}, 0
    else:
        by_severity = {str(k): int(v) for k, v in issues_df.groupby("severity").size().items()}
        by_rule = {str(k): int(v) for k, v in issues_df.groupby("rule_id").size().items()}
        affected_rows = int(issues_df.loc[issues_df["row_index"].notna(), "row_index"].nunique())

    validation_summary = {
        "input_rows": int(len(df)),
        "input_columns": int(len(df.columns)),
        "total_issues": int(len(issues_df)),
        "affected_rows": affected_rows,
        "issues_by_severity": by_severity,
        "issues_by_rule": by_rule,
        "dispositions": {
            "accept": int((df["validation_disposition"] == "ACCEPT").sum()),
            "warning": int((df["validation_disposition"] == "WARNING").sum()),
            "quarantine": int((df["validation_disposition"] == "QUARANTINE").sum())
        }
    }

    # 9. Export Artifacts
    print(f"\n[10/10] Exporting validation artifacts to: {output_dir.resolve()}...")
    df.to_csv(output_dir / "news_validation_all.csv", index=False, encoding="utf-8-sig")
    accepted_df.to_csv(output_dir / "news_accepted.csv", index=False, encoding="utf-8-sig")
    warning_df.to_csv(output_dir / "news_warning_review.csv", index=False, encoding="utf-8-sig")
    quarantine_df.to_csv(output_dir / "news_quarantine.csv", index=False, encoding="utf-8-sig")
    issues_df.to_csv(output_dir / "news_validation_issues.csv", index=False, encoding="utf-8-sig")
    completeness.to_csv(output_dir / "news_field_completeness.csv", index=False, encoding="utf-8-sig")

    with open(output_dir / "news_validation_summary.json", "w", encoding="utf-8") as f:
        json.dump(validation_summary, f, indent=2, ensure_ascii=False)

    print("\nSaved artifacts:")
    for p in sorted(output_dir.iterdir()):
        print(f" - {p.name:32s} ({p.stat().st_size:,} bytes)")

    print("\nSUCCESS: Day 8 Data Validation Pipeline completed successfully!\n")
    return validation_summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run LimeX News Validation Pipeline")
    parser.add_argument("--input", default=None, help="Input clean CSV path")
    parser.add_argument("--output-dir", default="validation_output", help="Output directory")
    parser.add_argument("--sample", type=int, default=None, help="Sample size (rows)")
    args = parser.parse_args()

    run_validation(input_path=args.input, output_dir=args.output_dir, sample_size=args.sample)
