"""
eda_news.py  Day 9: Exploratory Data Analysis
LimeX AI/ML Foundation Internship

Categories (from 1.txt): health, politics, stock, market, energy, technology
Also adds 'domain' column from URL and 'category' column via keyword matching.

Run:  python eda_news.py
"""
from __future__ import annotations
import argparse, json, sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent.parent

CANDIDATE_INPUTS = [
    HERE.parent / "Task 5 data validation and quality assessment" / "validation_output" / "news_accepted.csv",
    HERE.parent / "Task 4 data preprocessing pipeline" / "preprocessed_output" / "news_clean.csv",
    REPO_ROOT / "day 4" / "output_bulk" / "final" / "articles_bulk_new.csv",
]

CATEGORY_KEYWORDS = {
    "health":     ["health","medical","hospital","disease","drug","vaccine","pandemic","doctor","patient","pharma"],
    "politics":   ["politics","election","government","president","parliament","senate","vote","policy","minister"],
    "stock":      ["stock","shares","equity","nasdaq","nyse","ipo","dividend","earnings"],
    "market":     ["market","economy","gdp","inflation","trade","export","import","fiscal","monetary"],
    "energy":     ["energy","oil","gas","coal","solar","wind","renewable","power","electricity","fuel"],
    "technology": ["technology","tech","ai","machine learning","software","hardware","startup","cyber","cloud","digital"],
}


def parse_args():
    p = argparse.ArgumentParser(description="LimeX Day 9 EDA")
    p.add_argument("--input",      default=None)
    p.add_argument("--output-dir", default=str(HERE / "eda_output"))
    return p.parse_args()


# ── 1. Load ───────────────────────────────────────────────────────────────
def load_dataset(input_path):
    if input_path:
        p = Path(input_path)
        if not p.exists():
            sys.exit(f"[ERROR] Not found: {p}")
        print(f"\n[1/12] Loading: {p}")
        return pd.read_csv(p), p
    for c in CANDIDATE_INPUTS:
        if c.exists():
            print(f"\n[1/12] Auto-detected: {c}")
            return pd.read_csv(c), c
    sys.exit("[ERROR] No CSV found. Run Day 7 preprocessing first or pass --input.")


# ── 2. Detect columns ─────────────────────────────────────────────────────
def detect_cols(df):
    cols = set(df.columns)
    def pick(*cands):
        for c in cands:
            if c in cols: return c
        return None
    return {
        "url":       pick("news_url_clean","news_url","url"),
        "title":     pick("title_clean","title"),
        "content":   pick("content_clean","content","words"),
        "author":    pick("author_clean","author"),
        "published": pick("published_at_utc","published_time","date"),
        "scraped":   pick("scraped_at"),
        "domain":    pick("domain","source"),
        "category":  pick("category","article_niche","channel"),
    }


# ── 3. Enrich ─────────────────────────────────────────────────────────────
def enrich(df, COLS):
    print("\n[2/12] Adding category, domain & text-length columns...")
    import re

    # domain
    if not COLS["domain"] and COLS["url"]:
        def exdom(u):
            if not isinstance(u,str): return None
            m = re.search(r"(?:https?://)?(?:www\.)?([^/\s?#]+)", u)
            return m.group(1).lower() if m else None
        df["domain"] = df[COLS["url"]].apply(exdom)
        COLS["domain"] = "domain"

    # category
    if not COLS["category"]:
        def assign_cat(row):
            text = " ".join(filter(None,[
                str(row.get(COLS["title"] or "","") or ""),
                str(row.get(COLS["content"] or "","") or ""),
            ])).lower()
            for cat,kws in CATEGORY_KEYWORDS.items():
                if any(k in text for k in kws): return cat
            return "other"
        print("       Assigning categories from keywords (title+content)...")
        df["category"] = df.apply(assign_cat, axis=1)
        COLS["category"] = "category"

    # text lengths (use precomputed if present)
    if "title_word_count" in df.columns:
        df["_title_words"] = df["title_word_count"]
        df["_title_chars"] = df.get("title_char_count", pd.Series(0,index=df.index))
    elif COLS["title"]:
        df["_title_words"] = df[COLS["title"]].astype("string").str.split().str.len().fillna(0).astype(int)
        df["_title_chars"] = df[COLS["title"]].astype("string").str.len().fillna(0).astype(int)

    if "content_word_count" in df.columns:
        df["_content_words"] = df["content_word_count"]
        df["_content_chars"] = df.get("content_char_count", pd.Series(0,index=df.index))
    elif COLS["content"]:
        df["_content_words"] = df[COLS["content"]].astype("string").str.split().str.len().fillna(0).astype(int)
        df["_content_chars"] = df[COLS["content"]].astype("string").str.len().fillna(0).astype(int)

    return df


# ── 4. Overview ───────────────────────────────────────────────────────────
def section_overview(df):
    print("\n[3/12] Schema overview...")
    overview = pd.DataFrame({
        "dtype":       df.dtypes.astype(str),
        "non_null":    df.notna().sum(),
        "missing":     df.isna().sum(),
        "missing_pct": (df.isna().mean()*100).round(2),
        "unique":      df.nunique(),
        "sample":      [str(df[c].dropna().iloc[0])[:60] if df[c].notna().any() else "" for c in df.columns],
    })
    print(f"   Rows: {len(df):,}  Cols: {df.shape[1]:,}  Exact dupes: {df.duplicated().sum():,}  "
          f"Memory: {df.memory_usage(deep=True).sum()/1024**2:.1f} MB")
    return overview


# ── 5. Missingness ────────────────────────────────────────────────────────
def section_missingness(df, COLS, out, overview):
    print("\n[4/12] Missingness analysis...")
    ms = overview[["missing","missing_pct"]].sort_values("missing_pct", ascending=False)
    to_plot = ms[ms["missing_pct"]>0]
    if len(to_plot):
        fig, ax = plt.subplots(figsize=(12,4))
        ax.bar(to_plot.index, to_plot["missing_pct"], color="#C44E52")
        ax.set_title("Missing values by column (%)"); ax.set_ylabel("Missing (%)"); ax.set_ylim(0,100)
        plt.xticks(rotation=60, ha="right"); plt.tight_layout()
        fig.savefig(out/"missing_values.png", dpi=100); plt.close(fig)
    return ms


# ── 6. Duplicates ─────────────────────────────────────────────────────────
def section_duplicates(df, COLS):
    print("\n[5/12] Duplicate analysis...")
    stats = {
        "exact_duplicate_rows":     int(df.duplicated().sum()),
        "duplicate_url_rows":       int(df[COLS["url"]].duplicated(keep=False).sum()) if COLS["url"] else None,
        "duplicate_title_rows":     int(df[COLS["title"]].astype("string").str.strip().duplicated(keep=False).sum()) if COLS["title"] else None,
        "duplicate_content_rows":   int(df[COLS["content"]].astype("string").str.strip().duplicated(keep=False).sum()) if COLS["content"] else None,
    }
    for k,v in stats.items(): print(f"   {k}: {v}")
    return stats


# ── 7. Text lengths ───────────────────────────────────────────────────────
def section_text_lengths(df, out):
    print("\n[6/12] Text-length distributions...")
    lcols = [c for c in ["_title_chars","_title_words","_content_chars","_content_words"] if c in df.columns]
    if not lcols: return
    df[lcols].describe(percentiles=[.01,.05,.25,.5,.75,.95,.99]).T.to_csv(out/"text_length_stats.csv")
    for col in lcols:
        vals = pd.to_numeric(df[col], errors="coerce").dropna()
        if not len(vals): continue
        fig, ax = plt.subplots(figsize=(9,4))
        ax.hist(vals.clip(upper=vals.quantile(.99)), bins=40, color="#4C72B0", edgecolor="white")
        ax.set_title(f"Distribution: {col}"); ax.set_xlabel(col); ax.set_ylabel("Articles")
        plt.tight_layout(); fig.savefig(out/f"dist_{col}.png", dpi=100); plt.close(fig)


# ── 8. Domain ─────────────────────────────────────────────────────────────
def section_domain(df, COLS, out):
    print("\n[7/12] Domain distribution...")
    if not COLS["domain"]: print("   No domain column."); return None
    sc = df[COLS["domain"]].fillna("(none)").value_counts()
    sc.rename("articles").to_csv(out/"source_distribution.csv")
    fig, ax = plt.subplots(figsize=(11,6))
    sc.head(20).sort_values().plot(kind="barh", ax=ax, color="#4C72B0")
    ax.set_title("Top 20 domains by article count"); ax.set_xlabel("Articles")
    plt.tight_layout(); fig.savefig(out/"domain_distribution.png", dpi=100); plt.close(fig)
    if "_content_words" in df.columns:
        df.groupby(COLS["domain"], dropna=False).agg(
            articles=("_content_words","size"),
            median_words=("_content_words","median"),
            mean_words=("_content_words","mean"),
            p95_words=("_content_words",lambda s:s.quantile(.95))
        ).query("articles>=3").sort_values("articles",ascending=False).round(1).to_csv(out/"domain_content_length.csv")
    return sc


# ── 9. Category ───────────────────────────────────────────────────────────
def section_category(df, COLS, out):
    print("\n[8/12] Category distribution...")
    if not COLS["category"]: print("   No category column."); return None
    cc = df[COLS["category"]].fillna("other").value_counts()
    cc.rename("articles").to_csv(out/"category_distribution.csv")
    fig, ax = plt.subplots(figsize=(10,5))
    cc.sort_values().plot(kind="barh", ax=ax, color="#55A868")
    ax.set_title("Article count by category"); ax.set_xlabel("Articles")
    plt.tight_layout(); fig.savefig(out/"category_distribution.png", dpi=100); plt.close(fig)
    if COLS["domain"] and df[COLS["category"]].nunique()<=20:
        mix = pd.crosstab(df[COLS["domain"]], df[COLS["category"]], normalize="index")*100
        fig2, ax2 = plt.subplots(figsize=(12, max(4, len(mix.head(15))*0.5)))
        mix.head(15).plot(kind="bar", stacked=True, ax=ax2)
        ax2.set_title("Category mix by domain (%)"); ax2.set_ylabel("Share (%)")
        plt.xticks(rotation=60, ha="right"); plt.tight_layout()
        fig2.savefig(out/"category_by_domain.png", dpi=100); plt.close(fig2)
    return cc


# ── 10. Time ──────────────────────────────────────────────────────────────
def section_time(df, COLS, out):
    print("\n[9/12] Publication-time patterns...")
    if not COLS["published"]: print("   No published timestamp."); return
    df["_pub_dt"] = pd.to_datetime(df[COLS["published"]], errors="coerce", utc=True)
    valid = df["_pub_dt"].dropna()
    print(f"   Valid timestamps: {len(valid):,} / {len(df):,}")
    if not len(valid): return
    df["_pub_date"] = df["_pub_dt"].dt.floor("D")
    df["_pub_hour"] = df["_pub_dt"].dt.hour
    df["_pub_day"]  = df["_pub_dt"].dt.day_name()
    # daily
    fig,ax = plt.subplots(figsize=(12,4))
    df.groupby("_pub_date").size().plot(ax=ax, color="#C44E52")
    ax.set_title("Articles per day (UTC)"); ax.set_xlabel("Date"); ax.set_ylabel("Articles")
    plt.tight_layout(); fig.savefig(out/"articles_per_day.png", dpi=100); plt.close(fig)
    # hourly
    hr = df["_pub_hour"].value_counts().sort_index().reindex(range(24), fill_value=0)
    fig,ax = plt.subplots(figsize=(10,4))
    hr.plot(kind="bar", ax=ax, color="#8172B2")
    ax.set_title("Articles by hour (UTC)"); ax.set_xlabel("Hour"); ax.set_ylabel("Articles")
    plt.tight_layout(); fig.savefig(out/"articles_per_hour.png", dpi=100); plt.close(fig)
    # weekday
    order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
    wd = df["_pub_day"].value_counts().reindex(order, fill_value=0)
    fig,ax = plt.subplots(figsize=(9,4))
    wd.plot(kind="bar", ax=ax, color="#DD8452")
    ax.set_title("Articles by weekday (UTC)"); ax.set_xlabel("Day"); ax.set_ylabel("Articles")
    plt.tight_layout(); fig.savefig(out/"articles_per_weekday.png", dpi=100); plt.close(fig)
    # scrape delay
    if COLS["scraped"]:
        df["_scr_dt"]  = pd.to_datetime(df[COLS["scraped"]], errors="coerce", utc=True)
        df["_delay_m"] = (df["_scr_dt"] - df["_pub_dt"]).dt.total_seconds()/60
        delay = df["_delay_m"].dropna()
        if len(delay):
            cl = delay.clip(lower=delay.quantile(.01), upper=delay.quantile(.99))
            fig,ax = plt.subplots(figsize=(9,4))
            ax.hist(cl, bins=40, color="#4C72B0"); ax.set_title("Publish-to-scrape delay (min)")
            ax.set_xlabel("Minutes"); ax.set_ylabel("Articles")
            plt.tight_layout(); fig.savefig(out/"scrape_delay.png", dpi=100); plt.close(fig)


# ── 11. Relationships ─────────────────────────────────────────────────────
def section_relationships(df, out):
    print("\n[10/12] Relationships...")
    if "_title_words" not in df.columns or "_content_words" not in df.columns: return
    pair = df[["_title_words","_content_words"]].dropna()
    if len(pair)<2: return
    r = round(pair.corr(method="pearson").iloc[0,1], 3)
    print(f"   Pearson r (title vs content words): {r}")
    samp = pair.sample(min(len(pair),5000), random_state=42)
    fig,ax = plt.subplots(figsize=(7,5))
    ax.scatter(samp["_title_words"], samp["_content_words"], alpha=.4, s=8, color="#4C72B0")
    ax.set_title(f"Title vs content words  (r={r})"); ax.set_xlabel("Title words"); ax.set_ylabel("Content words")
    plt.tight_layout(); fig.savefig(out/"title_vs_content.png", dpi=100); plt.close(fig)


# ── 12. Suspicious ────────────────────────────────────────────────────────
def section_suspicious(df, COLS):
    print("\n[11/12] Suspicious extraction candidates...")
    flag = pd.Series(False, index=df.index)
    if "_content_words" in df.columns:
        flag |= df["_content_words"].fillna(0).lt(40)
        flag |= df["_content_words"].gt(df["_content_words"].quantile(.99))
    if COLS["title"]:
        pat = r"access denied|captcha|robot check|page not found|403|enable javascript"
        flag |= df[COLS["title"]].astype("string").str.contains(pat, case=False, na=False)
    if "_delay_m" in df.columns:
        flag |= df["_delay_m"].lt(0).fillna(False)
    rcols = [c for c in [COLS["domain"],COLS["title"],COLS["published"],"_title_words","_content_words",COLS["url"]] if c and c in df.columns]
    out_df = df.loc[flag, rcols].copy()
    print(f"   Flagged: {len(out_df):,} / {len(df):,} ({len(out_df)/max(len(df),1)*100:.1f}%)")
    return out_df


# ── 13. Correlation ───────────────────────────────────────────────────────
def section_correlation(df, out):
    print("\n[12/12] Numeric correlation matrix...")
    num = df.select_dtypes(include=np.number)[[c for c in df.select_dtypes(include=np.number).columns if not c.startswith("Unnamed")]]
    if num.shape[1]<2: print("   <2 numeric columns — skip."); return
    corr = num.corr(numeric_only=True)
    n = len(corr.columns)
    fig,ax = plt.subplots(figsize=(max(6,n*.7), max(5,n*.6)))
    im = ax.imshow(corr, aspect="auto", vmin=-1, vmax=1, cmap="coolwarm")
    ax.set_xticks(range(n)); ax.set_xticklabels(corr.columns, rotation=60, ha="right")
    ax.set_yticks(range(n)); ax.set_yticklabels(corr.index)
    ax.set_title("Numeric feature correlation"); fig.colorbar(im, ax=ax)
    plt.tight_layout(); fig.savefig(out/"correlation_matrix.png", dpi=100); plt.close(fig)
    corr.to_csv(out/"correlation_matrix.csv")


# ── Export ────────────────────────────────────────────────────────────────
def export_all(df, COLS, overview, ms, dup_stats, sc, cc, sus_df, out):
    overview.to_csv(out/"column_profile.csv")
    ms.to_csv(out/"missingness_summary.csv")
    pd.Series(dup_stats, name="row_count").to_csv(out/"duplicate_summary.csv")
    sus_df.to_csv(out/"suspicious_candidates.csv", index=False)
    summary = {
        "rows": int(len(df)), "columns": int(df.shape[1]),
        "exact_duplicate_rows": dup_stats.get("exact_duplicate_rows"),
        "suspicious_candidates": int(len(sus_df)),
        "missing_pct_by_column": {str(k):float(v) for k,v in overview["missing_pct"].items()},
        "detected_fields": {k:v for k,v in COLS.items()},
        "outputs_directory": str(out.resolve()),
    }
    with open(out/"eda_summary.json","w",encoding="utf-8") as f:
        json.dump(summary,f,indent=2,ensure_ascii=False)
    print(f"\n[DONE] Outputs: {out.resolve()}")
    print("Files:", sorted(p.name for p in out.iterdir()))


# ── MAIN ──────────────────────────────────────────────────────────────────
def main():
    args = parse_args()
    out = Path(args.output_dir); out.mkdir(parents=True, exist_ok=True)
    df, _ = load_dataset(args.input)
    COLS = detect_cols(df)
    print(f"   Field mapping: {COLS}")
    df          = enrich(df, COLS)
    overview    = section_overview(df)
    ms          = section_missingness(df, COLS, out, overview)
    dup_stats   = section_duplicates(df, COLS)
    section_text_lengths(df, out)
    sc          = section_domain(df, COLS, out)
    cc          = section_category(df, COLS, out)
    section_time(df, COLS, out)
    section_relationships(df, out)
    sus_df      = section_suspicious(df, COLS)
    section_correlation(df, out)
    export_all(df, COLS, overview, ms, dup_stats, sc, cc, sus_df, out)

if __name__ == "__main__":
    main()
