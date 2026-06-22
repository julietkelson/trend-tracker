"""
load_gsc_csv.py — Seed GSC_QUERY_DAILY from a Google Search Console UI export.

Use this as an alternative to gsc_pull.py if you don't yet have a GCP service
account, or for a one-off retrospective load.

How to get the export:
    Google Search Console → Performance → Search results
    → Export → Download CSV
    Download both "Queries" and "Chart" tabs.

The UI export gives aggregate totals per query, not daily breakdowns (that
requires the API). This script synthesizes plausible daily rows by spreading
impressions evenly across the date range, then optionally spiking the top-N
queries for the most recent days so trend detection fires on them.

Usage:
    python load_gsc_csv.py --queries /path/to/Queries.csv --chart /path/to/Chart.csv
    python load_gsc_csv.py --queries /path/to/Queries.csv --chart /path/to/Chart.csv --clear
"""

import argparse
import os
from datetime import date

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

# Top-N queries to spike (simulates recent trending — adjust to taste)
SPIKE_TOP_N  = 30
SPIKE_FACTOR = 5
SPIKE_DAYS   = 3

BRAND_TERMS = frozenset({
    # Add your own brand/site-name terms here so they're excluded
    # e.g. "my news site", "mynewssite", "mns"
})


def snowflake_conn():
    kwargs = dict(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        authenticator=os.environ.get("SNOWFLAKE_AUTHENTICATOR", "externalbrowser"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
    )
    if os.environ.get("SNOWFLAKE_ROLE"):
        kwargs["role"] = os.environ["SNOWFLAKE_ROLE"]
    return snowflake.connector.connect(**kwargs)


def ensure_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS GSC_QUERY_DAILY (
            DATE        DATE          NOT NULL,
            QUERY       VARCHAR(2000) NOT NULL,
            PAGE        VARCHAR(2000) NOT NULL,
            CLICKS      INTEGER,
            IMPRESSIONS INTEGER,
            CTR         FLOAT,
            POSITION    FLOAT,
            INGESTED_AT TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
        )
    """)


def build_daily_rows(queries_csv: str, chart_csv: str) -> pd.DataFrame:
    chart = pd.read_csv(chart_csv)
    chart.columns = chart.columns.str.strip().str.lower()
    chart["date"] = pd.to_datetime(chart["date"]).dt.date
    all_dates   = sorted(chart["date"].tolist())
    n_days      = len(all_dates)
    spike_start = all_dates[-SPIKE_DAYS]

    q = pd.read_csv(queries_csv)
    q.columns = ["query", "clicks", "impressions", "ctr", "position"]
    q["query"]       = q["query"].str.strip().str.lower()
    q["impressions"] = pd.to_numeric(q["impressions"], errors="coerce").fillna(0).astype(int)
    q["clicks"]      = pd.to_numeric(q["clicks"],      errors="coerce").fillna(0).astype(int)

    if BRAND_TERMS:
        q = q[~q["query"].isin(BRAND_TERMS)]

    q = q.reset_index(drop=True)
    spike_queries = set(q.nlargest(SPIKE_TOP_N, "impressions")["query"])

    print(f"Date range: {all_dates[0]} → {all_dates[-1]}  ({n_days} days)")
    print(f"Queries: {len(q):,} | Spike candidates: {SPIKE_TOP_N}, factor={SPIKE_FACTOR}×, start {spike_start}")

    rows = []
    for _, row in q.iterrows():
        query        = row["query"]
        daily_impr   = max(1, round(int(row["impressions"]) / n_days))
        daily_clicks = max(0, round(int(row["clicks"])      / n_days))

        for d in all_dates:
            is_spike = (query in spike_queries) and (d >= spike_start)
            impr     = daily_impr   * SPIKE_FACTOR if is_spike else daily_impr
            clicks   = daily_clicks * SPIKE_FACTOR if is_spike else daily_clicks
            rows.append({
                "DATE":        d.strftime("%Y-%m-%d"),
                "QUERY":       query,
                "PAGE":        "https://www.yoursite.com/",
                "CLICKS":      clicks,
                "IMPRESSIONS": impr,
                "CTR":         round(clicks / impr, 4) if impr else 0.0,
                "POSITION":    float(row["position"]) if pd.notna(row["position"]) else 0.0,
            })

    df = pd.DataFrame(rows)
    print(f"Built {len(df):,} query-day rows")
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries", required=True, help="Path to Queries.csv from GSC export")
    parser.add_argument("--chart",   required=True, help="Path to Chart.csv from GSC export")
    parser.add_argument("--clear",   action="store_true",
                        help="Truncate GSC_QUERY_DAILY instead of loading")
    args = parser.parse_args()

    conn = snowflake_conn()
    cur  = conn.cursor()
    ensure_table(cur)
    conn.commit()

    if args.clear:
        cur.execute("TRUNCATE TABLE GSC_QUERY_DAILY")
        conn.commit()
        print("Truncated GSC_QUERY_DAILY")
        cur.close()
        conn.close()
        return

    df = build_daily_rows(args.queries, args.chart)

    cur.execute("TRUNCATE TABLE GSC_QUERY_DAILY")
    conn.commit()

    success, _, nrows, _ = write_pandas(
        conn, df, "GSC_QUERY_DAILY", auto_create_table=False, overwrite=False
    )
    if not success:
        raise RuntimeError("write_pandas failed")
    conn.commit()

    print(f"Loaded {nrows:,} rows into GSC_QUERY_DAILY")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
