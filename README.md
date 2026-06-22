# Trend Tracker

Detects rising Google Search Console queries and measures how quickly your newsroom covers them.


## What it does

1. Pulls daily search query impressions from Google Search Console
2. Detects queries trending above their 14-day rolling baseline (3× spike for 2+ consecutive days)
3. Matches each trending query to published articles using two stages:
   - **Jaccard similarity** on headline + URL slug tokens (fast pre-filter at ≥0.05)
   - **Snowflake Cortex COMPLETE** (`mistral-7b`) for ambiguous matches — handles nicknames, semantic gaps, and named entity confusion that keyword matching misses
4. Writes results to Snowflake with coverage gap status

## How it works

```
Google Search Console
        │
        ▼
  GSC_QUERY_DAILY          ← one row per query per day
        │
        ▼
  detect_trends()
    14-day rolling baseline per query
    flag: impressions ≥ 3× baseline, 2+ consecutive days
        │
        ▼
  match_trends_to_articles()
    Stage 1: Jaccard pre-filter (≥0.05) on headline + URL slug
    Stage 2: Cortex COMPLETE yes/no — handles "Wolves" → Timberwolves,
             "Ant" → Anthony Edwards, semantic ambiguity
    Fallback: Jaccard ≥0.15 if Cortex unavailable
        │
        ▼
  TREND_PUBLISH_GAP        ← one row per trend × article pair
  TREND_GAP_BEST_MATCH     ← view: best match per trend (for dashboards)
  TRENDING_NOT_COVERED     ← view: story gaps from the last 14 days
```

## Output schema

`TREND_PUBLISH_GAP` — one row per trending query × matching article:

| Column | Description |
|---|---|
| `QUERY` | The trending search term |
| `TREND_ONSET_DATE` | First day of the spike |
| `PEAK_IMPRESSIONS` | Max daily impressions during the spike |
| `SPIKE_RATIO` | How many times above the 14-day baseline |
| `ARTICLE_ID` | Unique article identifier from your CMS |
| `HEADLINE` | Matched article headline |
| `PRIMARY_SECTION` | Content section (Sports, News, etc.) |
| `AUTHOR_BYLINES` | Article byline(s) |
| `CANONICAL_URL` | Article URL |
| `PUBLISH_DATE` | Article publish timestamp (UTC) |
| `GAP_HOURS` | Hours between trend onset and article publish. Negative = ahead of trend. |
| `GAP_STATUS` | One of: `ahead_of_trend` / `proactive` / `same_day_before` / `within_24h` / `within_3_days` / `within_week` / `late` / `no_coverage` |
| `MATCH_SCORE` | Jaccard similarity score (kept for auditing, regardless of Cortex outcome) |
| `CALCULATED_AT` | When this pipeline run wrote the row |

See `examples/sample_output.csv` for representative data.

## Setup

### Requirements

- Python 3.11+
- Snowflake account with write access to a schema
- **Snowflake Cortex enabled** on your account — verify with `SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-7b', 'hi')` in a worksheet. If it errors, ask your Snowflake admin to enable Cortex. The pipeline falls back to pure Jaccard matching if Cortex is unavailable, so it will still run — just with lower match quality on ambiguous queries.
- Your CMS articles in a Snowflake table (see `load_arc()` in `pipeline.py` for required columns and how to adapt it to your schema)
- Google Search Console access (API or CSV export)

### Install

```bash
git clone https://github.com/julietkelson/trend-tracker
cd trend-tracker
pip install -r requirements.txt
cp .env.example .env
# edit .env with your credentials
```

### Configure your `.env`

```bash
# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_AUTHENTICATOR=externalbrowser   # or snowflake_jwt for unattended runs
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_ROLE=                           # optional

# CMS
ARC_TABLE=ARC_ARTICLES                    # Snowflake table with your published articles

# Publication context (used in the Cortex prompt for better classification)
PUBLICATION_NAME=The Star Tribune
PUBLICATION_LOCATION=Minnesota            # city, state, or region — leave blank if national

# GSC (only needed for gsc_pull.py API path)
GSC_SERVICE_ACCOUNT_FILE=/path/to/service_account.json
GSC_SITE_URL=https://www.yoursite.com/
```

### Load GSC data

**Option A — API (recommended for ongoing use)**

Requires a GCP service account with Search Console read access: https://developers.google.com/webmaster-tools/v1/how-tos/authorizing

```bash
# First run: backfill last 90 days
python gsc_pull.py --backfill

# Daily (add to cron or scheduler)
python gsc_pull.py
```

**Option B — CSV export (good for a retrospective one-off)**

In Google Search Console: Performance → Export → Download CSV. Download both the Queries and Chart tabs.

```bash
python load_gsc_csv.py \
  --queries /path/to/Queries.csv \
  --chart   /path/to/Chart.csv
```

This synthesizes daily rows from the aggregate export by spreading impressions evenly and spiking the top 30 queries for the last 3 days so trend detection fires. Add `--clear` to truncate the table before loading.

### Run the pipeline

```bash
python pipeline.py
```

### Run tests

```bash
pytest tests/
```

All tests use mocked Snowflake connections — no credentials needed.

## Cost

Snowflake Cortex COMPLETE (`mistral-7b`) at ~30 trending queries per run costs **less than $0.001 per pipeline run**. Cortex is only called for articles that clear the Jaccard pre-filter, so the call volume stays small. At daily cadence, expect under $0.40/year in Cortex costs.

Warehouse compute is shared with the rest of your Snowflake usage and depends on your tier.

## Scheduling

By default the pipeline uses `externalbrowser` SSO auth, which requires a browser popup. For unattended runs, switch to key-pair auth in `.env`:

```bash
SNOWFLAKE_AUTHENTICATOR=snowflake_jwt
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/rsa_key.p8
```

Then add `python pipeline.py` to cron, Airflow, or whatever scheduler your org uses.

## Adapting for your newsroom

**CMS table:** The only Star Tribune-specific piece is `load_arc()` in `pipeline.py`, which assumes Arc Publishing column names. If your articles are in Snowflake, point `ARC_TABLE` in `.env` at your table and adjust the column names in `load_arc()` to match your schema. Required fields are documented in that function.

**Publication context:** Set `PUBLICATION_NAME` and `PUBLICATION_LOCATION` in `.env`. These feed into the Cortex prompt to improve classification accuracy — "The Star Tribune is a Minnesota newspaper" resolves ambiguity that "a newspaper" cannot.

**Evergreen sections:** Update `EVERGREEN_SECTIONS` at the top of `pipeline.py` to match your section names. Sections like Obituaries, Games, and Weather are excluded from gap analysis since they're not news-driven.

## License

MIT — see [LICENSE](LICENSE).
