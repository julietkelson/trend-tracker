"""
gsc_pull.py — Pull Google Search Console data into Snowflake via the API.

Requires a GCP service account with Search Console read access.
See: https://developers.google.com/webmaster-tools/v1/how-tos/authorizing

Usage:
    python gsc_pull.py --backfill   # first run: loads last 90 days
    python gsc_pull.py              # daily cron: loads yesterday
"""

import os
import argparse
import logging
from datetime import date, timedelta

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
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

SNOWFLAKE_ACCOUNT       = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER          = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_AUTHENTICATOR = os.environ.get("SNOWFLAKE_AUTHENTICATOR", "externalbrowser")
SNOWFLAKE_WAREHOUSE     = os.environ["SNOWFLAKE_WAREHOUSE"]
SNOWFLAKE_DATABASE      = os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_SCHEMA        = os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_ROLE          = os.environ.get("SNOWFLAKE_ROLE")

GSC_SCOPES  = ["https://www.googleapis.com/auth/webmasters.readonly"]
ROW_LIMIT   = 25000   # hard API cap per request
DELAY_DAYS  = 3       # GSC data has a 2-3 day reporting lag


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


# ── Snowflake helpers ─────────────────────────────────────────────────────────

def snowflake_conn():
    kwargs = dict(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        authenticator=SNOWFLAKE_AUTHENTICATOR,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    if SNOWFLAKE_ROLE:
        kwargs["role"] = SNOWFLAKE_ROLE
    return snowflake.connector.connect(**kwargs)


def ensure_gsc_table(cur):
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


def write_day(conn, cur, rows: list[dict], pull_date: date):
    """Delete existing rows for this date then insert fresh (idempotent)."""
    if not rows:
        return
    date_str = pull_date.strftime("%Y-%m-%d")
    cur.execute("DELETE FROM GSC_QUERY_DAILY WHERE DATE = %s", (date_str,))
    df = pd.DataFrame(rows)
    success, _, nrows, _ = write_pandas(
        conn, df, "GSC_QUERY_DAILY", auto_create_table=False, overwrite=False
    )
    if not success:
        raise RuntimeError(f"write_pandas failed for {date_str}")
    log.info(f"  {date_str}: wrote {nrows:,} rows")


# ── Main ──────────────────────────────────────────────────────────────────────

def iter_dates(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description="Pull GSC data into Snowflake")
    parser.add_argument("--backfill", action="store_true",
                        help="Pull the last 90 days (run once on first setup)")
    args = parser.parse_args()

    end_date   = date.today() - timedelta(days=DELAY_DAYS)
    start_date = (end_date - timedelta(days=89)) if args.backfill else end_date

    log.info(f"Pulling {start_date} → {end_date}  (backfill={args.backfill})")

    service = build_gsc_service()
    conn    = snowflake_conn()
    cur     = conn.cursor()

    try:
        ensure_gsc_table(cur)
        conn.commit()
        for pull_date in iter_dates(start_date, end_date):
            rows = pull_one_day(service, pull_date)
            write_day(conn, cur, rows, pull_date)
            conn.commit()
    except Exception:
        log.exception("Pull failed")
        raise
    finally:
        cur.close()
        conn.close()

    log.info("Done ✓")


if __name__ == "__main__":
    main()
