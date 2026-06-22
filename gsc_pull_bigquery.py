"""
gsc_pull_bigquery.py — Pull Google Search Console data into BigQuery via the API.

UNTESTED PORT — Mirrors gsc_pull.py but writes to BigQuery instead of
Snowflake. The Snowflake version is what's been run in production.
This BigQuery mirror has not been run end-to-end — expect to debug.

Requires a GCP service account with both Search Console read access AND
BigQuery write access to BQ_DATASET. The same service account can be used
for both.

Usage:
    python gsc_pull_bigquery.py --backfill   # first run: loads last 90 days
    python gsc_pull_bigquery.py              # daily cron: loads yesterday
"""

import os
import argparse
import logging
from datetime import date, timedelta

import pandas as pd
from google.cloud import bigquery
from googleapiclient.discovery import build
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration (all from .env) ────────────────────────────────────────────

GSC_SERVICE_ACCOUNT_FILE = os.environ["GSC_SERVICE_ACCOUNT_FILE"]
GSC_SITE_URL             = os.environ["GSC_SITE_URL"]

GCP_PROJECT              = os.environ["GCP_PROJECT"]
BQ_LOCATION              = os.environ.get("BQ_LOCATION", "US")
BQ_DATASET               = os.environ.get("BQ_DATASET", "trend_tracker")

GSC_SCOPES  = ["https://www.googleapis.com/auth/webmasters.readonly"]
ROW_LIMIT   = 25000   # hard API cap per request
DELAY_DAYS  = 3       # GSC data has a 2-3 day reporting lag

TABLE_ID = f"{GCP_PROJECT}.{BQ_DATASET}.GSC_QUERY_DAILY"


# ── GSC helpers ───────────────────────────────────────────────────────────────

def build_gsc_service():
    creds = service_account.Credentials.from_service_account_file(
        GSC_SERVICE_ACCOUNT_FILE, scopes=GSC_SCOPES
    )
    return build("searchconsole", "v1", credentials=creds, cache_discovery=False)


def pull_one_day(service, pull_date: date) -> list[dict]:
    """Pull all query+page rows for a single day, paginating if needed."""
    date_str  = pull_date.strftime("%Y-%m-%d")
    rows      = []
    start_row = 0

    while True:
        response = service.searchanalytics().query(
            siteUrl=GSC_SITE_URL,
            body={
                "startDate":  date_str,
                "endDate":    date_str,
                "dimensions": ["query", "page"],
                "rowLimit":   ROW_LIMIT,
                "startRow":   start_row,
                "dataState":  "final",
            },
        ).execute()

        batch = response.get("rows", [])
        if not batch:
            break

        for r in batch:
            rows.append({
                "DATE":        date_str,
                "QUERY":       r["keys"][0],
                "PAGE":        r["keys"][1],
                "CLICKS":      int(r.get("clicks", 0)),
                "IMPRESSIONS": int(r.get("impressions", 0)),
                "CTR":         float(r.get("ctr", 0.0)),
                "POSITION":    float(r.get("position", 0.0)),
            })

        if len(batch) < ROW_LIMIT:
            break
        start_row += ROW_LIMIT

    log.info(f"  {date_str}: {len(rows):,} rows")
    return rows


# ── BigQuery helpers ──────────────────────────────────────────────────────────

def bq_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT, location=BQ_LOCATION)


def ensure_dataset(client: bigquery.Client):
    dataset_ref = bigquery.Dataset(f"{GCP_PROJECT}.{BQ_DATASET}")
    dataset_ref.location = BQ_LOCATION
    client.create_dataset(dataset_ref, exists_ok=True)


def ensure_gsc_table(client: bigquery.Client):
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


def write_day(client: bigquery.Client, rows: list[dict], pull_date: date):
    """Delete existing rows for this date then insert fresh (idempotent)."""
    if not rows:
        return

    date_str = pull_date.strftime("%Y-%m-%d")
    client.query(
        f"DELETE FROM `{TABLE_ID}` WHERE DATE = DATE('{date_str}')"
    ).result()

    df = pd.DataFrame(rows)
    df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
    df["INGESTED_AT"] = pd.Timestamp.utcnow()

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_dataframe(df, TABLE_ID, job_config=job_config)
    job.result()
    log.info(f"  {date_str}: wrote {len(df):,} rows")


# ── Main ──────────────────────────────────────────────────────────────────────

def iter_dates(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description="Pull GSC data into BigQuery")
    parser.add_argument("--backfill", action="store_true",
                        help="Pull the last 90 days (run once on first setup)")
    args = parser.parse_args()

    end_date   = date.today() - timedelta(days=DELAY_DAYS)
    start_date = (end_date - timedelta(days=89)) if args.backfill else end_date

    log.info(f"Pulling {start_date} → {end_date}  (backfill={args.backfill})")

    service = build_gsc_service()
    client  = bq_client()

    ensure_dataset(client)
    ensure_gsc_table(client)

    for pull_date in iter_dates(start_date, end_date):
        rows = pull_one_day(service, pull_date)
        write_day(client, rows, pull_date)

    log.info("Done ✓")


if __name__ == "__main__":
    main()
