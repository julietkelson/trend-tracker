import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from unittest.mock import MagicMock
from datetime import date, timedelta

import pandas as pd
import pytest

from pipeline import tokenize, jaccard


# ── helpers ───────────────────────────────────────────────────────────────────

def _mock_conn(rows):
    """rows: list of (idx, answer) tuples returned by Snowflake."""
    cur = MagicMock()
    cur.fetchall.return_value = rows
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def _candidates(*headlines):
    """Build minimal candidate dicts for cortex_classify tests."""
    onset = date.today()
    return [
        {
            "score":    0.10,
            "headline": h,
            "section":  "Sports",
            "art": pd.Series({
                "article_id":      str(i),
                "headline":        h,
                "primary_section": "Sports",
                "author_bylines":  "Staff",
                "canonical_url":   f"https://www.example.com/{i}",
                "display_date":    pd.Timestamp(onset, tz="UTC"),
            }),
        }
        for i, h in enumerate(headlines)
    ]


def _make_trends(query="timberwolves"):
    onset = date.today() - timedelta(days=3)
    return pd.DataFrame([{
        "query":            query,
        "trend_onset_date": onset,
        "peak_impressions": 500,
        "spike_ratio":      5.0,
    }])


def _make_articles(*headlines):
    onset = date.today() - timedelta(days=3)
    rows = []
    for i, h in enumerate(headlines):
        rows.append({
            "article_id":      str(i),
            "headline":        h,
            "dek":             "",
            "url_slug":        h.lower().replace(" ", "-"),
            "display_date":    pd.Timestamp(onset, tz="UTC"),
            "primary_section": "Sports",
            "author_bylines":  "Staff",
            "canonical_url":   f"https://www.example.com/{i}",
        })
    return pd.DataFrame(rows)


# ── smoke tests ───────────────────────────────────────────────────────────────

def test_tokenize_removes_stop_words():
    tokens = tokenize("the Timberwolves beat the Nuggets")
    assert "the" not in tokens
    assert "timberwolves" in tokens
    assert "nuggets" in tokens


def test_jaccard_identical_sets():
    a = frozenset({"timberwolves", "win"})
    assert jaccard(a, a) == 1.0


def test_jaccard_disjoint_sets():
    a = frozenset({"timberwolves"})
    b = frozenset({"nuggets"})
    assert jaccard(a, b) == 0.0


# ── cortex_classify ───────────────────────────────────────────────────────────

from pipeline import cortex_classify


def test_cortex_classify_returns_yes_candidates():
    candidates = _candidates("Wolves beat Nuggets", "Chicken nugget recipe", "Wolves title run")
    conn = _mock_conn([(0, "yes"), (1, "no"), (2, "yes")])
    result = cortex_classify(conn, "timberwolves", candidates)
    assert len(result) == 2
    assert result[0]["headline"] == "Wolves beat Nuggets"
    assert result[1]["headline"] == "Wolves title run"


def test_cortex_classify_empty_candidates_skips_db():
    conn = _mock_conn([])
    result = cortex_classify(conn, "timberwolves", [])
    assert result == []
    conn.cursor.assert_not_called()


def test_cortex_classify_returns_none_on_failure():
    conn = MagicMock()
    conn.cursor.return_value.execute.side_effect = Exception("Cortex unavailable")
    result = cortex_classify(conn, "timberwolves", _candidates("Some article"))
    assert result is None


def test_cortex_classify_case_insensitive_yes():
    conn = _mock_conn([(0, "Yes, this article covers the topic.")])
    result = cortex_classify(conn, "timberwolves", _candidates("Wolves win"))
    assert len(result) == 1


def test_cortex_classify_rejects_all_no():
    conn = _mock_conn([(0, "no"), (1, "no")])
    result = cortex_classify(conn, "timberwolves", _candidates("Recipe ideas", "Weather forecast"))
    assert result == []


# ── match_trends_to_articles ──────────────────────────────────────────────────

from pipeline import match_trends_to_articles


def test_cortex_confirms_low_jaccard_match():
    """Article with Jaccard 0.05–0.15 is matched when Cortex says yes."""
    conn = _mock_conn([(0, "yes")])
    trends   = _make_trends("timberwolves")
    articles = _make_articles(
        "NBA trade rumors teams including timberwolves interested in big free agent"
    )
    result  = match_trends_to_articles(trends, articles, conn=conn)
    matched = result[result["GAP_STATUS"] != "no_coverage"]
    assert len(matched) == 1


def test_cortex_rejects_low_jaccard_candidate():
    """Article with Jaccard 0.05–0.15 is NOT matched when Cortex says no."""
    conn = _mock_conn([(0, "no")])
    trends   = _make_trends("timberwolves")
    articles = _make_articles(
        "NBA trade rumors teams including timberwolves interested in big free agent"
    )
    result  = match_trends_to_articles(trends, articles, conn=conn)
    matched = result[result["GAP_STATUS"] != "no_coverage"]
    assert len(matched) == 0


def test_fallback_to_jaccard_when_cortex_fails():
    """When Cortex raises, articles with Jaccard >= 0.15 still match."""
    conn = MagicMock()
    conn.cursor.return_value.execute.side_effect = Exception("unavailable")
    trends   = _make_trends("timberwolves")
    articles = _make_articles("timberwolves win playoffs")
    result   = match_trends_to_articles(trends, articles, conn=conn)
    matched  = result[result["GAP_STATUS"] != "no_coverage"]
    assert len(matched) == 1


def test_no_conn_uses_jaccard_only():
    """conn=None uses pure Jaccard at 0.15 — backward-compatible default."""
    trends   = _make_trends("timberwolves")
    articles = _make_articles(
        "timberwolves win playoffs",
        "NBA trade rumors teams including timberwolves interested in big free agent",
    )
    result  = match_trends_to_articles(trends, articles, conn=None)
    matched = result[result["GAP_STATUS"] != "no_coverage"]
    assert len(matched) == 1
    assert matched.iloc[0]["HEADLINE"] == "timberwolves win playoffs"
