"""
pipeline.py — Newsroom trend gap detector.

Reads:
    GSC_QUERY_DAILY      (Snowflake, written by gsc_pull.py or load_gsc_csv.py)
    ARC_TABLE            (Snowflake, your CMS articles — set ARC_TABLE in .env)

Writes:
    TREND_PUBLISH_GAP    (one row per trending-query × matching-article pair)
    TRENDING_NOT_COVERED (view — trends with zero matching stories)
    TREND_GAP_BEST_MATCH (view — best match per trend, for dashboards)

Usage:
    python pipeline.py
"""

import os
import re
import logging
from datetime import date, timedelta

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ─────────────────────────────────────────────────────────────

# Snowflake credentials are read inside snowflake_conn() so the module can be
# imported in tests without a live .env file.
ARC_TABLE            = os.environ.get("ARC_TABLE", "ARC_ARTICLES")
PUBLICATION_NAME     = os.environ.get("PUBLICATION_NAME", "our publication")
PUBLICATION_LOCATION = os.environ.get("PUBLICATION_LOCATION", "")

# ── Trend detection thresholds ────────────────────────────────────────────────
SPIKE_RATIO_MIN     = 3.0   # query must reach 3× its rolling baseline
IMPRESSIONS_MIN     = 50    # ignore tiny-volume queries (noise floor)
ROLLING_WINDOW_DAYS = 14    # days to compute the baseline average
CONSECUTIVE_DAYS    = 2     # minimum days of spiking before we call it a trend

# ── Topic matching config ─────────────────────────────────────────────────────
MATCH_WINDOW_BEFORE  = 7     # look for stories published up to N days before trend onset
MATCH_WINDOW_AFTER   = 14    # look for stories published up to N days after trend onset
MATCH_SCORE_MIN      = 0.05  # Jaccard pre-filter threshold (feeds Cortex)
MATCH_SCORE_FALLBACK = 0.15  # Jaccard threshold used when Cortex is unavailable
CORTEX_MODEL         = "mistral-7b"

# Sections that are always "trending" — exclude from gap analysis.
# Update these to match your publication's section names.
EVERGREEN_SECTIONS = frozenset({"Obituaries", "Games", "Puzzles", "Weather", "Wires"})

# Common stop words — stripped before keyword matching
STOP_WORDS = frozenset({
    "a", "an", "the", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "was", "are", "were", "be", "been",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "can", "this", "that", "it", "its",
    "he", "she", "we", "they", "what", "who", "how", "when", "where",
})


# ── Snowflake helpers ─────────────────────────────────────────────────────────

def snowflake_conn():
    kwargs = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR", "externalbrowser"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
    )
    role = os.environ.get("SNOWFLAKE_ROLE")
    if role:
        kwargs["role"] = role
    return snowflake.connector.connect(**kwargs)


# ── Step 1: Load data ─────────────────────────────────────────────────────────

def load_gsc(conn) -> pd.DataFrame:
    """
    Load the last 93 days of GSC data.
    Returns an empty DataFrame if GSC_QUERY_DAILY doesn't exist yet.
    """
    cutoff = (date.today() - timedelta(days=93)).strftime("%Y-%m-%d")
    try:
        df = pd.read_sql(f"""
            SELECT
                DATE,
                LOWER(QUERY) AS QUERY,
                SUM(CLICKS)      AS CLICKS,
                SUM(IMPRESSIONS) AS IMPRESSIONS
            FROM GSC_QUERY_DAILY
            WHERE DATE >= '{cutoff}'
            GROUP BY DATE, LOWER(QUERY)
            ORDER BY DATE, QUERY
        """, conn)
    except Exception as exc:
        if "does not exist" in str(exc).lower() or "not authorized" in str(exc).lower():
            log.warning("GSC_QUERY_DAILY not found — run gsc_pull.py or load_gsc_csv.py first")
            return pd.DataFrame(columns=["date", "query", "clicks", "impressions"])
        raise

    df.columns = df.columns.str.lower()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    log.info(f"Loaded {len(df):,} query-day rows from GSC")
    return df


def load_arc(conn) -> pd.DataFrame:
    """
    Load articles published in the last 120 days.

    CUSTOMIZE THIS FOR YOUR CMS:
    The query below assumes Arc Publishing column names. Rename columns to
    match your schema. The pipeline requires these fields downstream:
        - article_id      — unique identifier (any string)
        - headline        — article title (used for keyword matching)
        - url_slug        — URL path component, hyphens replaced with spaces
                            for tokenization. Use '' if you don't have one.
        - display_date    — publish timestamp, must be parseable as ISO8601
        - primary_section — content category (used in Cortex prompt)
        - author_bylines  — stored in output, not used for matching
        - canonical_url   — stored in output, not used for matching

    Also update:
        - The WHERE clause filter (PUBLISHED_STATUS = 'PUBLISHED') to match
          your CMS's published/draft distinction
        - EVERGREEN_SECTIONS at the top of this file to match your section names
    """
    cutoff = (date.today() - timedelta(days=120)).strftime("%Y-%m-%d")
    section_exclusions = ", ".join(f"'{s}'" for s in EVERGREEN_SECTIONS)

    df = pd.read_sql(f"""
        SELECT
            ARTICLE_ID,
            HEADLINE,
            DEK,
            URL_SLUG,
            DISPLAY_DATE,
            PRIMARY_SECTION,
            AUTHOR_BYLINES,
            CANONICAL_URL
        FROM {ARC_TABLE}
        WHERE PUBLISHED_STATUS = 'PUBLISHED'
          AND DISPLAY_DATE >= '{cutoff}'
          AND PRIMARY_SECTION NOT IN ({section_exclusions})
          AND HEADLINE IS NOT NULL
    """, conn)

    df.columns = df.columns.str.lower()
    df["display_date"] = pd.to_datetime(df["display_date"], format="ISO8601", utc=True)
    log.info(f"Loaded {len(df):,} Arc articles")
    return df


# ── Step 2: Detect trending queries ───────────────────────────────────────────

def detect_trends(gsc: pd.DataFrame) -> pd.DataFrame:
    """
    Flag queries spiking >= 3× their 14-day rolling baseline for 2+ consecutive days.
    Returns one row per trend onset event.
    """
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
    """Extract meaningful keywords, removing stop words."""
    if not text:
        return frozenset()
    tokens = re.findall(r"\b[a-z]{2,}\b", str(text).lower())
    return frozenset(t for t in tokens if t not in STOP_WORDS)


def jaccard(set_a: frozenset, set_b: frozenset) -> float:
    """Jaccard similarity: size of intersection / size of union."""
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


def classify_gap(gap_hours: float) -> str:
    """Bucket the publish-to-trend gap for dashboards."""
    if gap_hours < -72:
        return "ahead_of_trend"    # published 3+ days before trend
    if gap_hours < -24:
        return "proactive"         # published 1–3 days before trend
    if gap_hours < 0:
        return "same_day_before"
    if gap_hours <= 24:
        return "within_24h"
    if gap_hours <= 72:
        return "within_3_days"
    if gap_hours <= 168:
        return "within_week"
    return "late"


def cortex_classify(conn, query: str, candidates: list) -> list | None:
    """
    Ask Snowflake Cortex COMPLETE whether each candidate article covers the query.
    Returns the subset of candidates where Cortex answers "yes".
    Returns None on any exception so callers can fall back to Jaccard.
    """
    if not candidates:
        return []

    try:
        cur = conn.cursor()
        values_parts = []
        for i, c in enumerate(candidates):
            headline = c["headline"].replace("'", "''")
            section  = c.get("section", "").replace("'", "''")
            q        = query.replace("'", "''")
            location_clause = f" based in {PUBLICATION_LOCATION}" if PUBLICATION_LOCATION else ""
            prompt = (
                f"{PUBLICATION_NAME} is a news publication{location_clause}. "
                f"A reader searched for \"{q}\". "
                f"Did {PUBLICATION_NAME} publish an article specifically about this topic?\\n"
                f"Headline: \"{headline}\"\\n"
                f"Section: {section}\\n"
                f"Answer yes or no."
            ).replace("'", "''")
            values_parts.append(f"({i}, '{prompt}')")

        values_sql = ",\n    ".join(values_parts)
        sql = f"""
            SELECT idx_col,
                   SNOWFLAKE.CORTEX.COMPLETE('{CORTEX_MODEL}', prompt_col) AS cortex_answer
            FROM VALUES
                {values_sql}
            AS t(idx_col, prompt_col)
        """
        cur.execute(sql)
        rows = cur.fetchall()

        return [
            candidates[idx]
            for idx, answer in rows
            if isinstance(answer, str) and answer.strip().lower().startswith("yes")
        ]

    except Exception:
        log.warning("Cortex classify failed for query '%s'", query, exc_info=True)
        return None


def match_trends_to_articles(
    trends: pd.DataFrame,
    articles: pd.DataFrame,
    conn=None,
) -> pd.DataFrame:
    """
    Two-stage matching for each trend:
      Stage 1: Jaccard pre-filter (>= 0.05) on headline + URL slug tokens
      Stage 2: Cortex yes/no confirmation for ambiguous matches
      Fallback: pure Jaccard >= 0.15 if Cortex fails or conn is None
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

        if conn is None:
            confirmed = [c for c in pre_filtered if c["score"] >= MATCH_SCORE_FALLBACK]
        else:
            cortex_result = cortex_classify(conn, query, pre_filtered)
            if cortex_result is None:
                confirmed = [c for c in pre_filtered if c["score"] >= MATCH_SCORE_FALLBACK]
            else:
                confirmed = cortex_result

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


# ── Step 4: Write results to Snowflake ────────────────────────────────────────

def ensure_output_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS TREND_PUBLISH_GAP (
            QUERY             VARCHAR(2000),
            TREND_ONSET_DATE  DATE,
            SPIKE_RATIO       FLOAT,
            PEAK_IMPRESSIONS  INTEGER,
            ARTICLE_ID        VARCHAR,
            HEADLINE          VARCHAR(2000),
            PRIMARY_SECTION   VARCHAR,
            AUTHOR_BYLINES    VARCHAR,
            CANONICAL_URL     VARCHAR(2000),
            PUBLISH_DATE      TIMESTAMP_TZ,
            GAP_HOURS         FLOAT,
            MATCH_SCORE       FLOAT,
            GAP_STATUS        VARCHAR,
            CALCULATED_AT     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    cur.execute("""
        CREATE OR REPLACE VIEW TREND_GAP_BEST_MATCH AS
        SELECT * EXCLUDE (rn)
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY QUERY, TREND_ONSET_DATE
                       ORDER BY MATCH_SCORE DESC, GAP_HOURS ASC
                   ) AS rn
            FROM TREND_PUBLISH_GAP
            WHERE GAP_STATUS != 'no_coverage'
        )
        WHERE rn = 1
    """)

    cur.execute("""
        CREATE OR REPLACE VIEW TRENDING_NOT_COVERED AS
        SELECT
            QUERY,
            TREND_ONSET_DATE,
            SPIKE_RATIO,
            PEAK_IMPRESSIONS,
            DATEDIFF('hour', TREND_ONSET_DATE, CURRENT_TIMESTAMP) AS HOURS_SINCE_ONSET,
            CALCULATED_AT
        FROM TREND_PUBLISH_GAP
        WHERE GAP_STATUS = 'no_coverage'
          AND TREND_ONSET_DATE >= DATEADD('day', -14, CURRENT_DATE)
        ORDER BY TREND_ONSET_DATE DESC, PEAK_IMPRESSIONS DESC
    """)


def write_results(conn, results: pd.DataFrame):
    cur = conn.cursor()
    try:
        ensure_output_tables(cur)
        conn.commit()

        cur.execute("DELETE FROM TREND_PUBLISH_GAP WHERE CALCULATED_AT::DATE = CURRENT_DATE")
        conn.commit()

        if results.empty:
            log.info("No results to write")
            return

        results = results.copy()
        if "TREND_ONSET_DATE" in results.columns:
            results["TREND_ONSET_DATE"] = pd.to_datetime(results["TREND_ONSET_DATE"]).dt.date

        success, _, nrows, _ = write_pandas(
            conn, results, "TREND_PUBLISH_GAP",
            auto_create_table=False, overwrite=False,
            use_logical_type=True,
        )
        if not success:
            raise RuntimeError("write_pandas failed for TREND_PUBLISH_GAP")

        conn.commit()
        log.info(f"Wrote {nrows:,} rows to TREND_PUBLISH_GAP")
    finally:
        cur.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    conn = snowflake_conn()
    try:
        gsc      = load_gsc(conn)
        articles = load_arc(conn)
        trends   = detect_trends(gsc)

        if trends.empty:
            log.info("No trend signals detected — nothing to write")
            write_results(conn, pd.DataFrame())
            return

        results = match_trends_to_articles(trends, articles, conn=conn)
        write_results(conn, results)

        if not results.empty:
            summary = results.groupby("GAP_STATUS").size().sort_values(ascending=False)
            log.info("Gap status summary:\n" + summary.to_string())

    except Exception:
        log.exception("Pipeline failed")
        raise
    finally:
        conn.close()

    log.info("Done ✓")


if __name__ == "__main__":
    main()
