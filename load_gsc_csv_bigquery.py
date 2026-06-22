"""
load_gsc_csv_bigquery.py — Seed GSC_QUERY_DAILY (BigQuery) from a Google
Search Console UI export.

UNTESTED PORT — Mirrors load_gsc_csv.py but writes to BigQuery instead
of Snowflake. The Snowflake version is what's been run in production.
This BigQuery mirror has not been run end-to-end — expect to debug.

Use this as an alternative to gsc_pull_bigquery.py if you don't yet have
a GCP service account configured for Search Console, or for a one-off
retrospective load.

How to get the export:
    Google Search Console → Performance → Search results
    → Export → Download CSV
    Download both "Queries" and "Chart" tabs.

The UI export gives aggregate totals per query, not daily breakdowns (that
requires the API). This script synthesizes plausible daily rows by spreading
impressions evenly across the date range, then spiking the top-N queries
for the most recent days so trend detection fires on them.

Usage:
    python load_gsc_csv_bigquery.py --queries /path/to/Queries.csv --chart /path/to/Chart.csv
    python load_gsc_csv_bigquery.py --queries /path/to/Queries.csv --chart /path/to/Chart.csv --clear
"""

import argparse
import os

import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT = os.environ["GCP_PROJECT"]
BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")
BQ_DATASET  = os.environ.get("BQ_DATASET", "trend_tracker")
TABLE_ID    = f"{GCP_PROJECT}.{BQ_DATASET}.GSC_QUERY_DAILY"

# Top-N queries to spike (simulates recent trending — adjust to taste)
SPIKE_TOP_N  = 30
SPIKE_FACTOR = 5
SPIKE_DAYS   = 3

BRAND_TERMS = frozenset({
    # Add your own brand/site-name terms here so they're excluded
    # e.g. "my news site", "mynewssite", "mns"
})


def bq_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)


def ensure_dataset(client: bigquery.Client):
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}")
    dataset_ref.location = BQ_LOCATION
    client.create_dataset(dataset_ref, exists_ok=True)


def ensure_table(client: bigquery.Client):
    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{TABLE_ID}` (
            DATE        DATE      NOT NULL,
            QUERY       STRING    NOT NULL,
            PAGE        STRING    NOT NULL,
            CLICKS      INT64,
            IMPRESSIONS INT64,
            CTR         FLOAT64,
            POSITION    FLOAT64,
            INGESTED_AT TIMESTAMP
        )
    """).result()


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
                "DATE":        d,
                "QUERY":       query,
                "PAGE":        "https://www.yoursite.com/",
                "CLICKS":      clicks,
                "IMPRESSIONS": impr,
                "CTR":         round(clicks / impr, 4) if impr else 0.0,
                "POSITION":    float(row["position"]) if pd.notna(row["position"]) else 0.0,
            })

    df = pd.DataFrame(rows)
    df["INGESTED_AT"] = pd.Timestamp.utcnow()
    print(f"Built {len(df):,} query-day rows")
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries", required=True, help="Path to Queries.csv from GSC export")
    parser.add_argument("--chart",   required=True, help="Path to Chart.csv from GSC export")
    parser.add_argument("--clear",   action="store_true",
                        help="Truncate GSC_QUERY_DAILY instead of loading")
    args = parser.parse_args()

    client = bq_client()
    ensure_dataset(client)
    ensure_table(client)

    if args.clear:
        client.query(f"TRUNCATE TABLE `{TABLE_ID}`").result()
        print("Truncated GSC_QUERY_DAILY")
        return

    df = build_daily_rows(args.queries, args.chart)

    client.query(f"TRUNCATE TABLE `{TABLE_ID}`").result()

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()

    print(f"Loaded {len(df):,} rows into GSC_QUERY_DAILY")


if __name__ == "__main__":
    main()
