"""
pipeline_bigquery.py — Newsroom trend gap detector (BigQuery port).

UNTESTED PORT — This file mirrors pipeline.py but targets BigQuery instead
of Snowflake. The Snowflake version (pipeline.py) is what's been run in
production. This BigQuery mirror is provided as a starting point for BQ
users and has not been run end-to-end. Expect to debug.

Reads:
    {BQ_DATASET}.GSC_QUERY_DAILY  (written by gsc_pull_bigquery.py
                                   or load_gsc_csv_bigquery.py)
    BQ_ARC_TABLE                  (your CMS articles — set in .env)

Writes:
    {BQ_DATASET}.TREND_PUBLISH_GAP     (one row per trend × article pair)
    {BQ_DATASET}.TRENDING_NOT_COVERED  (view — trends with zero matches)
    {BQ_DATASET}.TREND_GAP_BEST_MATCH  (view — best match per trend)

LLM step:
    Uses BigQuery ML.GENERATE_TEXT with a remote Gemini model. Requires
    one-time setup of a BQ Connection and CREATE MODEL — see README.
    Falls back to pure Jaccard matching if BQ_GEMINI_MODEL is unset or
    the remote model is unavailable.

Usage:
    python pipeline_bigquery.py
"""

import os
import re
import logging
from datetime import date, timedelta

import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

GCP_PROJECT          = os.environ.get("GCP_PROJECT")
BQ_LOCATION          = os.environ.get("BQ_LOCATION", "US")
BQ_DATASET           = os.environ.get("BQ_DATASET", "trend_tracker")
BQ_ARC_TABLE         = os.environ.get("BQ_ARC_TABLE", "")
BQ_GEMINI_MODEL      = os.environ.get("BQ_GEMINI_MODEL", "")
PUBLICATION_NAME     = os.environ.get("PUBLICATION_NAME", "our publication")
PUBLICATION_LOCATION = os.environ.get("PUBLICATION_LOCATION", "")

GSC_TABLE            = f"`{GCP_PROJECT}.{BQ_DATASET}.GSC_QUERY_DAILY`"
GAP_TABLE            = f"`{GCP_PROJECT}.{BQ_DATASET}.TREND_PUBLISH_GAP`"
NOT_COVERED_VIEW     = f"`{GCP_PROJECT}.{BQ_DATASET}.TRENDING_NOT_COVERED`"
BEST_MATCH_VIEW      = f"`{GCP_PROJECT}.{BQ_DATASET}.TREND_GAP_BEST_MATCH`"

# ── Trend detection thresholds (identical to Snowflake version) ───────────────
SPIKE_RATIO_MIN     = 3.0
IMPRESSIONS_MIN     = 50
ROLLING_WINDOW_DAYS = 14
CONSECUTIVE_DAYS    = 2

# ── Topic matching config ─────────────────────────────────────────────────────
MATCH_WINDOW_BEFORE  = 7
MATCH_WINDOW_AFTER   = 14
MATCH_SCORE_MIN      = 0.05
MATCH_SCORE_FALLBACK = 0.15

EVERGREEN_SECTIONS = frozenset({"Obituaries", "Games", "Puzzles", "Weather", "Wires"})

STOP_WORDS = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "was", "are", "were", "be", "been",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "can", "this", "that", "it", "its",
    "he", "she", "we", "they", "what", "who", "how", "when", "where",
})


# ── BigQuery helpers ──────────────────────────────────────────────────────────

def bq_client() -> bigquery.Client:
    if not GCP_PROJECT:
        raise RuntimeError("GCP_PROJECT not set — see .env.example")
    return bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)


# ── Step 1: Load data ─────────────────────────────────────────────────────────

def load_gsc(client: bigquery.Client) -> pd.DataFrame:
    """Load the last 93 days of GSC data."""
    cutoff = (date.today() - timedelta(days=93)).strftime("%Y-%m-%d")
    try:
        df = client.query(f"""
            SELECT
                DATE,
                LOWER(QUERY) AS QUERY,
                SUM(CLICKS)      AS CLICKS,
                SUM(IMPRESSIONS) AS IMPRESSIONS
            FROM {GSC_TABLE}
            WHERE DATE >= DATE('{cutoff}')
            GROUP BY DATE, LOWER(QUERY)
            ORDER BY DATE, QUERY
        """).to_dataframe()
    except Exception as exc:
        if "not found" in str(exc).lower():
            log.warning("GSC_QUERY_DAILY not found — run gsc_pull_bigquery.py or load_gsc_csv_bigquery.py first")
            return pd.DataFrame(columns=["date", "query", "clicks", "impressions"])
        raise

    df.columns = df.columns.str.lower()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    log.info(f"Loaded {len(df):,} query-day rows from GSC")
    return df


def load_arc(client: bigquery.Client) -> pd.DataFrame:
    """
    Load articles published in the last 120 days.

    CUSTOMIZE THIS FOR YOUR CMS:
    The query below assumes Arc Publishing column names. Adjust to match
    your schema. Required downstream fields:
        - article_id, headline, url_slug, display_date, primary_section,
          author_bylines, canonical_url
    Also update the WHERE clause filter (PUBLISHED_STATUS = 'PUBLISHED')
    and EVERGREEN_SECTIONS at the top of this file.
    """
    if not BQ_ARC_TABLE:
        raise RuntimeError("BQ_ARC_TABLE not set — see .env.example")

    cutoff = (date.today() - timedelta(days=120)).strftime("%Y-%m-%d")
    section_exclusions = ", ".join(f"'{s}'" for s in EVERGREEN_SECTIONS)

    df = client.query(f"""
        SELECT
            ARTICLE_ID,
            HEADLINE,
            DEK,
            URL_SLUG,
            DISPLAY_DATE,
            PRIMARY_SECTION,
            AUTHOR_BYLINES,
            CANONICAL_URL
        FROM `{BQ_ARC_TABLE}`
        WHERE PUBLISHED_STATUS = 'PUBLISHED'
          AND DISPLAY_DATE >= TIMESTAMP('{cutoff}')
          AND PRIMARY_SECTION NOT IN ({section_exclusions})
          AND HEADLINE IS NOT NULL
    """).to_dataframe()

    df.columns = df.columns.str.lower()
    df["display_date"] = pd.to_datetime(df["display_date"], utc=True)
    log.info(f"Loaded {len(df):,} Arc articles")
    return df


# ── Step 2: Detect trending queries ───────────────────────────────────────────

def detect_trends(gsc: pd.DataFrame) -> pd.DataFrame:
    """Flag queries spiking >= 3× their 14-day rolling baseline for 2+ consecutive days."""
    log.info("Running trend detection...")

    pivot = (
        gsc.pivot_table(index="date", columns="query", values="impressions", fill_value=0)
        .sort_index()
    )
    baseline   = pivot.rolling(window=ROLLING_WINDOW_DAYS, min_periods=3).mean().shift(1)
    spike_ratio = pivot / baseline.replace(0, float("nan"))
    is_spiking  = (spike_ratio >= SPIKE_RATIO_MIN) & (pivot >= IMPRESSIONS_MIN)

    spiking_long = (
        is_spiking.reset_index()
        .melt(id_vars="date", var_name="query", value_name="spiking")
        .query("spiking == True")
        .drop(columns="spiking")
        .sort_values(["query", "date"])
        .reset_index(drop=True)
    )

    if spiking_long.empty:
        log.info("No spiking queries found")
        return pd.DataFrame(columns=["query", "trend_onset_date", "peak_impressions", "spike_ratio"])

    ratio_long = spike_ratio.reset_index().melt(id_vars="date", var_name="query", value_name="spike_ratio")
    impr_long  = pivot.reset_index().melt(id_vars="date", var_name="query", value_name="impressions")
    spiking_long = (
        spiking_long
        .merge(ratio_long, on=["date", "query"], how="left")
        .merge(impr_long,  on=["date", "query"], how="left")
    )

    trend_onsets = []
    for query, group in spiking_long.groupby("query"):
        dates = sorted(group["date"].tolist())
        if not dates:
            continue

        run_start = dates[0]
        run_rows  = [group[group["date"] == dates[0]].iloc[0]]
        prev      = dates[0]

        for d in dates[1:]:
            if (d - prev).days == 1:
                run_rows.append(group[group["date"] == d].iloc[0])
            else:
                if len(run_rows) >= CONSECUTIVE_DAYS:
                    run_df = pd.DataFrame(run_rows)
                    trend_onsets.append({
                        "query":            query,
                        "trend_onset_date": run_start,
                        "peak_impressions": int(run_df["impressions"].max()),
                        "spike_ratio":      round(float(run_df["spike_ratio"].max()), 2),
                    })
                run_start = d
                run_rows  = [group[group["date"] == d].iloc[0]]
            prev = d

        if len(run_rows) >= CONSECUTIVE_DAYS:
            run_df = pd.DataFrame(run_rows)
            trend_onsets.append({
                "query":            query,
                "trend_onset_date": run_start,
                "peak_impressions": int(run_df["impressions"].max()),
                "spike_ratio":      round(float(run_df["spike_ratio"].max()), 2),
            })

    trends = pd.DataFrame(trend_onsets)
    log.info(f"Found {len(trends)} trend onset events across {trends['query'].nunique()} unique queries")
    return trends


# ── Step 3: Match trending queries to articles ────────────────────────────────

def tokenize(text: str) -> frozenset:
    if not text:
        return frozenset()
    tokens = re.findall(r"\b[a-z]{2,}\b", str(text).lower())
    return frozenset(t for t in tokens if t not in STOP_WORDS)


def jaccard(set_a: frozenset, set_b: frozenset) -> float:
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


def classify_gap(gap_hours: float) -> str:
    if gap_hours < -72:
        return "ahead_of_trend"
    if gap_hours < -24:
        return "proactive"
    if gap_hours < 0:
        return "same_day_before"
    if gap_hours <= 24:
        return "within_24h"
    if gap_hours <= 72:
        return "within_3_days"
    if gap_hours <= 168:
        return "within_week"
    return "late"


def gemini_classify(client: bigquery.Client, query: str, candidates: list) -> list | None:
    """
    Ask BigQuery ML.GENERATE_TEXT (remote Gemini model) whether each candidate
    article covers the query. Returns the subset where the model answers "yes".
    Returns None on any exception so callers can fall back to Jaccard.
    Returns None immediately if BQ_GEMINI_MODEL is unset.
    """
    if not candidates:
        return []
    if not BQ_GEMINI_MODEL:
        return None

    try:
        struct_rows = []
        for i, c in enumerate(candidates):
            headline = c["headline"].replace("'", "\\'")
            section  = c.get("section", "").replace("'", "\\'")
            q        = query.replace("'", "\\'")
            location_clause = f" based in {PUBLICATION_LOCATION}" if PUBLICATION_LOCATION else ""
            prompt = (
                f"{PUBLICATION_NAME} is a news publication{location_clause}. "
                f"A reader searched for \"{q}\". "
                f"Did {PUBLICATION_NAME} publish an article specifically about this topic?\\n"
                f"Headline: \"{headline}\"\\n"
                f"Section: {section}\\n"
                f"Answer yes or no."
            )
            struct_rows.append(f"STRUCT({i} AS idx, '{prompt}' AS prompt)")

        struct_sql = ",\n        ".join(struct_rows)
        sql = f"""
            SELECT idx, ml_generate_text_llm_result AS answer
            FROM ML.GENERATE_TEXT(
                MODEL `{BQ_GEMINI_MODEL}`,
                (SELECT idx, prompt FROM UNNEST([
                    {struct_sql}
                ])),
                STRUCT(
                    0.0 AS temperature,
                    20  AS max_output_tokens,
                    TRUE AS flatten_json_output
                )
            )
        """
        rows = client.query(sql).result()

        confirmed_idx = {
            int(r["idx"])
            for r in rows
            if isinstance(r["answer"], str) and r["answer"].strip().lower().startswith("yes")
        }
        return [c for i, c in enumerate(candidates) if i in confirmed_idx]

    except Exception:
        log.warning("Gemini classify failed for query '%s'", query, exc_info=True)
        return None


def match_trends_to_articles(
    trends: pd.DataFrame,
    articles: pd.DataFrame,
    client: bigquery.Client | None = None,
) -> pd.DataFrame:
    """
    Two-stage matching for each trend:
      Stage 1: Jaccard pre-filter (>= 0.05) on headline + URL slug tokens
      Stage 2: Gemini yes/no confirmation for ambiguous matches
      Fallback: pure Jaccard >= 0.15 if Gemini fails or client is None
    """
    log.info(f"Matching {len(trends)} trends against {len(articles)} articles...")

    articles = articles.copy()
    articles["tokens"] = articles.apply(
        lambda r: tokenize(" ".join(filter(None, [
            str(r.get("headline", "")),
            str(r.get("url_slug", "")).replace("-", " "),
        ]))),
        axis=1,
    )

    matched_rows = []
    unmatched_queries = []

    for _, trend in trends.iterrows():
        query        = trend["query"]
        onset        = trend["trend_onset_date"]
        query_tokens = tokenize(query)

        if not query_tokens:
            continue

        window_start = pd.Timestamp(onset - timedelta(days=MATCH_WINDOW_BEFORE), tz="UTC")
        window_end   = pd.Timestamp(onset + timedelta(days=MATCH_WINDOW_AFTER),  tz="UTC")
        window_arts  = articles[
            (articles["display_date"] >= window_start) &
            (articles["display_date"] <= window_end)
        ]

        pre_filtered = [
            {
                "score":    jaccard(query_tokens, art["tokens"]),
                "headline": art["headline"],
                "section":  art.get("primary_section", ""),
                "art":      art,
            }
            for _, art in window_arts.iterrows()
            if jaccard(query_tokens, art["tokens"]) >= MATCH_SCORE_MIN
        ]

        if client is None:
            confirmed = [c for c in pre_filtered if c["score"] >= MATCH_SCORE_FALLBACK]
        else:
            gemini_result = gemini_classify(client, query, pre_filtered)
            if gemini_result is None:
                confirmed = [c for c in pre_filtered if c["score"] >= MATCH_SCORE_FALLBACK]
            else:
                confirmed = gemini_result

        best_score = 0.0
        for c in confirmed:
            art       = c["art"]
            score     = c["score"]
            gap_hours = (
                art["display_date"] - pd.Timestamp(onset, tz="UTC")
            ).total_seconds() / 3600

            matched_rows.append({
                "QUERY":            query,
                "TREND_ONSET_DATE": onset,
                "SPIKE_RATIO":      trend["spike_ratio"],
                "PEAK_IMPRESSIONS": trend["peak_impressions"],
                "ARTICLE_ID":       str(art["article_id"]),
                "HEADLINE":         art["headline"],
                "PRIMARY_SECTION":  art["primary_section"],
                "AUTHOR_BYLINES":   art["author_bylines"],
                "CANONICAL_URL":    art["canonical_url"],
                "PUBLISH_DATE":     art["display_date"],
                "GAP_HOURS":        round(gap_hours, 1),
                "MATCH_SCORE":      round(score, 3),
                "GAP_STATUS":       classify_gap(gap_hours),
            })
            best_score = max(best_score, score)

        if best_score == 0.0:
            unmatched_queries.append(trend)

    missed_rows = []
    for _, trend in pd.DataFrame(unmatched_queries).iterrows() if unmatched_queries else []:
        missed_rows.append({
            "QUERY":            trend["query"],
            "TREND_ONSET_DATE": trend["trend_onset_date"],
            "SPIKE_RATIO":      trend["spike_ratio"],
            "PEAK_IMPRESSIONS": trend["peak_impressions"],
            "ARTICLE_ID":       None,
            "HEADLINE":         None,
            "PRIMARY_SECTION":  None,
            "AUTHOR_BYLINES":   None,
            "CANONICAL_URL":    None,
            "PUBLISH_DATE":     None,
            "GAP_HOURS":        None,
            "MATCH_SCORE":      0.0,
            "GAP_STATUS":       "no_coverage",
        })

    result = pd.DataFrame(matched_rows + missed_rows)
    log.info(f"Matched: {len(matched_rows)} trend-article pairs | Uncovered: {len(missed_rows)} trends")
    return result


# ── Step 4: Write results to BigQuery ─────────────────────────────────────────

def ensure_dataset(client: bigquery.Client):
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}")
    dataset_ref.location = BQ_LOCATION
    client.create_dataset(dataset_ref, exists_ok=True)


def ensure_output_tables(client: bigquery.Client):
    client.query(f"""
        CREATE TABLE IF NOT EXISTS {GAP_TABLE} (
            QUERY             STRING,
            TREND_ONSET_DATE  DATE,
            SPIKE_RATIO       FLOAT64,
            PEAK_IMPRESSIONS  INT64,
            ARTICLE_ID        STRING,
            HEADLINE          STRING,
            PRIMARY_SECTION   STRING,
            AUTHOR_BYLINES    STRING,
            CANONICAL_URL     STRING,
            PUBLISH_DATE      TIMESTAMP,
            GAP_HOURS         FLOAT64,
            MATCH_SCORE       FLOAT64,
            GAP_STATUS        STRING,
            CALCULATED_AT     TIMESTAMP
        )
    """).result()

    client.query(f"""
        CREATE OR REPLACE VIEW {BEST_MATCH_VIEW} AS
        SELECT * EXCEPT (rn)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY QUERY, TREND_ONSET_DATE
                       ORDER BY MATCH_SCORE DESC, GAP_HOURS ASC
                   ) AS rn
            FROM {GAP_TABLE}
            WHERE GAP_STATUS != 'no_coverage'
        )
        WHERE rn = 1
    """).result()

    client.query(f"""
        CREATE OR REPLACE VIEW {NOT_COVERED_VIEW} AS
        SELECT
            QUERY,
            TREND_ONSET_DATE,
            SPIKE_RATIO,
            PEAK_IMPRESSIONS,
            TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), TIMESTAMP(TREND_ONSET_DATE), HOUR) AS HOURS_SINCE_ONSET,
            CALCULATED_AT
        FROM {GAP_TABLE}
        WHERE GAP_STATUS = 'no_coverage'
          AND TREND_ONSET_DATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        ORDER BY TREND_ONSET_DATE DESC, PEAK_IMPRESSIONS DESC
    """).result()


def write_results(client: bigquery.Client, results: pd.DataFrame):
    ensure_dataset(client)
    ensure_output_tables(client)

    client.query(
        f"DELETE FROM {GAP_TABLE} WHERE DATE(CALCULATED_AT) = CURRENT_DATE()"
    ).result()

    if results.empty:
        log.info("No results to write")
        return

    results = results.copy()
    if "TREND_ONSET_DATE" in results.columns:
        results["TREND_ONSET_DATE"] = pd.to_datetime(results["TREND_ONSET_DATE"]).dt.date
    results["CALCULATED_AT"] = pd.Timestamp.utcnow()

    table_id = f"{GCP_PROJECT}.{BQ_DATASET}.TREND_PUBLISH_GAP"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_dataframe(results, table_id, job_config=job_config)
    job.result()
    log.info(f"Wrote {len(results):,} rows to TREND_PUBLISH_GAP")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    client = bq_client()
    try:
        gsc      = load_gsc(client)
        articles = load_arc(client)
        trends   = detect_trends(gsc)

        if trends.empty:
            log.info("No trend signals detected — nothing to write")
            write_results(client, pd.DataFrame())
            return

        results = match_trends_to_articles(trends, articles, client=client)
        write_results(client, results)

        if not results.empty:
            summary = results.groupby("GAP_STATUS").size().sort_values(ascending=False)
            log.info("Gap status summary:\n" + summary.to_string())

    except Exception:
        log.exception("Pipeline failed")
        raise

    log.info("Done ✓")


if __name__ == "__main__":
    main()
