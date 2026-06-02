# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Social listening client — Google Trends + Reddit ingestion pipeline.

Two data sources:
  GoogleTrendsClient   — relative search interest via an authenticated marketing
                         data provider (DataForSEO or compatible). Falls back to
                         direct Google Trends Explore HTTP when no provider is
                         configured. Never uses pytrends.
  RedditClient         — posts and submissions via PRAW (official Python Reddit
                         API Wrapper). Requires REDDIT_CLIENT_ID /
                         REDDIT_CLIENT_SECRET / REDDIT_USER_AGENT credentials.
                         Enforces the 1,000-post listing hard limit.

Central text pipeline:
  normalize_text()     — strips HTML, tracking pixels, and emojis from raw
                         ingest payloads before they reach BigQuery.

Privacy contract:
  • Reddit author handles are SHA-256 hashed before storage — never raw.
  • IP addresses and device fingerprints are never collected or logged.
  • Sentiment scores are derived from normalised text only (no raw PII inputs).

Optional dependencies (install via pip install 'paid-media-agent[social]'):
  praw>=7.8.0       — Reddit API client
  textblob>=0.18.0  — sentiment scoring (graceful fallback if absent)
  requests>=2.32.0  — HTTP client for Google Trends adapter
"""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from base64 import b64encode
from datetime import date, datetime, timedelta, timezone
from typing import Any

import structlog

from config import settings
from tools import bigquery_client as bq

log = structlog.get_logger()

# ── Text normalisation helpers ─────────────────────────────────────────────────

# HTML tag stripper (including self-closing, script, and style blocks)
_RE_HTML_TAG        = re.compile(r'<[^>]{0,2000}>', re.DOTALL)
# Tracking pixel patterns: img/script URLs with dimension hints or tracking params
_RE_TRACKING_PIXEL  = re.compile(
    r'https?://\S{0,500}?[?&](utm_\w+|pixel|track|beacon|open)[^"\s]*',
    re.IGNORECASE,
)
# URL-encoded data URIs and base64 pixel blobs
_RE_DATA_URI        = re.compile(r'data:[^;]+;base64,[A-Za-z0-9+/=]+')
# Unicode emoji blocks (covers most common ranges without requiring third-party emoji lib)
_RE_EMOJI = re.compile(
    "["
    "\U0001F300-\U0001F5FF"   # Misc symbols and pictographs
    "\U0001F600-\U0001F64F"   # Emoticons
    "\U0001F680-\U0001F6FF"   # Transport and map symbols
    "\U0001F700-\U0001F77F"   # Alchemical symbols
    "\U0001F780-\U0001F7FF"   # Geometric shapes extended
    "\U0001F800-\U0001F8FF"   # Supplemental arrows-C
    "\U0001F900-\U0001F9FF"   # Supplemental symbols and pictographs
    "\U0001FA00-\U0001FA6F"   # Chess symbols
    "\U0001FA70-\U0001FAFF"   # Symbols and pictographs extended-A
    "\U00002600-\U000026FF"   # Misc symbols
    "\U00002700-\U000027BF"   # Dingbats
    "\U0000FE00-\U0000FE0F"   # Variation selectors
    "\U0001F1E0-\U0001F1FF"   # Flags (country indicators)
    "]+",
    re.UNICODE,
)
_RE_WHITESPACE      = re.compile(r'\s+')


def normalize_text(raw_text: str | None) -> str:
    """
    Scrub raw ingest text before database storage.

    Pipeline (applied in order):
      1. Strip data: URIs and base64 blobs
      2. Remove tracking pixel URLs
      3. Strip HTML tags (including script/style blocks)
      4. Remove Unicode emoji sequences
      5. Collapse whitespace runs to a single space
      6. Strip leading / trailing whitespace

    Args:
        raw_text: The raw text as captured from the source. None → "".

    Returns:
        Clean, plain-text string suitable for sentiment scoring and BQ storage.
    """
    if not raw_text:
        return ""
    text = raw_text
    text = _RE_DATA_URI.sub(" ", text)
    text = _RE_TRACKING_PIXEL.sub(" ", text)
    text = _RE_HTML_TAG.sub(" ", text)
    text = _RE_EMOJI.sub(" ", text)
    text = _RE_WHITESPACE.sub(" ", text)
    return text.strip()


# ── Sentiment scoring ──────────────────────────────────────────────────────────

# Minimal positive/negative word lexicon — used when textblob is unavailable.
# Based on a curated subset of the AFINN-111 lexicon (public domain).
_POS_WORDS = frozenset({
    "good", "great", "excellent", "amazing", "fantastic", "outstanding",
    "superb", "brilliant", "wonderful", "perfect", "love", "best", "top",
    "strong", "impressive", "innovative", "valuable", "efficient", "fast",
    "easy", "helpful", "useful", "reliable", "trusted", "winning", "growth",
    "improved", "better", "positive", "excited", "happy", "recommend",
})
_NEG_WORDS = frozenset({
    "bad", "terrible", "awful", "horrible", "poor", "worst", "broken",
    "slow", "fail", "failed", "failure", "difficult", "hard", "annoying",
    "frustrated", "disappointed", "useless", "expensive", "overpriced",
    "broken", "bug", "bugs", "crash", "crashes", "scam", "fraud",
    "misleading", "confusing", "complex", "ugly", "messy", "problem",
    "issues", "concern", "wrong", "error", "errors", "angry", "upset",
})


def _score_sentiment(text: str) -> float:
    """
    Return a sentiment polarity score in the range [-1.0, +1.0].

    Attempts to use textblob.TextBlob for higher-quality scoring.
    Falls back to a word-ratio lexicon approach if textblob is not installed.

    Args:
        text: Normalised (already scrubbed) plain text.

    Returns:
        Float in [-1.0, +1.0]. 0.0 = neutral / unknown.
    """
    if not text:
        return 0.0
    try:
        from textblob import TextBlob  # type: ignore[import]
        polarity: float = TextBlob(text).sentiment.polarity
        return round(polarity, 4)
    except ImportError:
        pass
    # Lexicon fallback: (pos_count - neg_count) / total_word_count
    tokens = text.lower().split()
    if not tokens:
        return 0.0
    pos = sum(1 for t in tokens if t in _POS_WORDS)
    neg = sum(1 for t in tokens if t in _NEG_WORDS)
    score = (pos - neg) / len(tokens)
    return round(max(-1.0, min(1.0, score * 10)), 4)  # scale up; cap at ±1


def _hash_author(author_name: str | None) -> str | None:
    """
    Return a SHA-256 hex digest of the author handle.

    Stored for within-run deduplication only — the original handle is never
    persisted in any table.
    """
    if not author_name or author_name in ("[deleted]", "[removed]"):
        return None
    return hashlib.sha256(author_name.encode()).hexdigest()


# ── Google Trends client ───────────────────────────────────────────────────────

_GOOGLE_TRENDS_DIRECT_URL = "https://trends.google.com/trends/api/explore"
_GOOGLE_TRENDS_TIMELINE_URL = "https://trends.google.com/trends/api/widgetdata/multiline"
_DIRECT_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://trends.google.com/",
}
_XSSI_PREFIX = ")]}'"


class GoogleTrendsClient:
    """
    Fetch relative search interest data from Google Trends.

    Priority order:
      1. Authenticated marketing data provider (DataForSEO-compatible API) if
         GOOGLE_TRENDS_PROVIDER_URL + credentials are configured in settings.
      2. Direct HTTP to trends.google.com/trends/api with retry / back-off.
         Not authenticated, but uses proper headers and rate limiting.
         Does NOT use pytrends or any unauthenticated scraping library.

    All responses are normalised to a list of {signal_date, keyword_string,
    geography_code, signal_weight_score (0–100), related_topics} dicts.
    """

    def __init__(self) -> None:
        try:
            import requests
            self._requests = requests
        except ImportError as exc:
            raise ImportError(
                "requests is required for GoogleTrendsClient. "
                "Install with: pip install 'paid-media-agent[social]'"
            ) from exc

        self._provider_url      = settings.google_trends_provider_url
        self._provider_username = settings.google_trends_provider_username
        self._provider_password = settings.google_trends_provider_password

        self._use_provider = bool(
            self._provider_url
            and self._provider_username
            and self._provider_password
        )

        if self._use_provider:
            log.info("google_trends.client_init", mode="provider", url=self._provider_url)
        else:
            log.info("google_trends.client_init", mode="direct_http")

    def fetch_interest_over_time(
        self,
        keywords: list[str],
        geo: str = "US",
        lookback_days: int = 30,
    ) -> list[dict]:
        """
        Return daily interest-over-time data for the given keywords.

        Args:
            keywords:     1–5 keyword strings. Google Trends compares up to 5 at a time.
            geo:          ISO 3166-1 alpha-2 country code (e.g. "US", "GB", "DE").
                          Empty string = worldwide.
            lookback_days: Number of days to look back from today.

        Returns:
            List of dicts:
              keyword_string, geography_code, signal_date (ISO str),
              signal_weight_score (float 0–100), related_topics (list | None)
        """
        if not keywords:
            return []
        # Google Trends supports max 5 keywords per request
        results: list[dict] = []
        for batch_start in range(0, len(keywords), 5):
            batch = keywords[batch_start : batch_start + 5]
            try:
                if self._use_provider:
                    batch_results = self._fetch_via_provider(batch, geo, lookback_days)
                else:
                    batch_results = self._fetch_via_direct_http(batch, geo, lookback_days)
                results.extend(batch_results)
            except Exception as exc:
                log.warning(
                    "google_trends.fetch_failed",
                    keywords=batch,
                    geo=geo,
                    error=str(exc),
                )
            if batch_start + 5 < len(keywords):
                time.sleep(1.5)  # courtesy pause between batches
        return results

    # ── Provider path (DataForSEO-compatible) ──────────────────────────────────

    def _fetch_via_provider(
        self,
        keywords: list[str],
        geo: str,
        lookback_days: int,
    ) -> list[dict]:
        """
        Fetch via authenticated marketing data provider (DataForSEO Google Trends API).

        DataForSEO endpoint:
          POST /v3/keywords_data/google_trends/explore/live
          Basic Auth: username=email, password=api_key

        Response schema: tasks[0].result[0].items — each item has:
          type="google_trends_graph", data=[{date_from, date_to, values:[{value}]}]
        """
        date_to   = date.today().isoformat()
        date_from = (date.today() - timedelta(days=lookback_days)).isoformat()

        # Location code mapping — DataForSEO uses numeric location codes
        _GEO_CODES = {
            "US": 2840, "GB": 2826, "CA": 2124, "AU": 2036, "DE": 2276,
            "FR": 2250, "SG": 2702, "IN": 2356, "BR": 2076, "MX": 2484,
        }
        location_code = _GEO_CODES.get(geo.upper(), 2840)

        payload = [
            {
                "keywords": keywords,
                "type": "web",
                "date_from": date_from,
                "date_to": date_to,
                "location_code": location_code,
                "language_code": "en",
            }
        ]

        credentials = b64encode(
            f"{self._provider_username}:{self._provider_password}".encode()
        ).decode()
        headers = {
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/json",
        }

        resp = self._requests.post(
            self._provider_url,
            headers=headers,
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        results: list[dict] = []
        for task in data.get("tasks", []):
            for result_item in task.get("result", []) or []:
                for item in result_item.get("items", []) or []:
                    if item.get("type") != "google_trends_graph":
                        continue
                    kw = item.get("keywords", [keywords[0]])[0] if "keywords" in item else keywords[0]
                    for point in item.get("data", []) or []:
                        vals = point.get("values", [])
                        score = float(vals[0]) if vals else 0.0
                        dt = point.get("date_from", date_from)
                        results.append({
                            "keyword_string":      kw,
                            "geography_code":      geo.upper(),
                            "signal_date":         dt,
                            "signal_weight_score": score,
                            "related_topics":      None,
                            "timeframe_label":     f"{date_from}/{date_to}",
                        })
        log.info(
            "google_trends.provider_fetch_ok",
            keywords=keywords,
            geo=geo,
            rows=len(results),
        )
        return results

    # ── Direct HTTP path ───────────────────────────────────────────────────────

    def _fetch_via_direct_http(
        self,
        keywords: list[str],
        geo: str,
        lookback_days: int,
    ) -> list[dict]:
        """
        Fetch directly from trends.google.com/trends/api.

        Uses two calls:
          1. /explore — returns a token and widget metadata for the keyword set
          2. /widgetdata/multiline — returns the actual time-series data using that token

        Handles the XSSI prefix (")]}'" or similar) in every response.
        Rate limited to 1 request per 2 seconds per batch.
        """
        # Build timeframe string
        if lookback_days <= 7:
            timeframe = "now 7-d"
        elif lookback_days <= 30:
            timeframe = "today 1-m"
        elif lookback_days <= 90:
            timeframe = "today 3-m"
        else:
            timeframe = "today 12-m"

        comparison_items = [
            {"keyword": kw, "geo": geo, "time": timeframe}
            for kw in keywords
        ]
        req_param = json.dumps(
            {"comparisonItem": comparison_items, "category": 0, "property": ""},
            separators=(",", ":"),
        )

        explore_resp = self._requests.get(
            _GOOGLE_TRENDS_DIRECT_URL,
            params={"hl": "en-US", "tz": 0, "req": req_param},
            headers=_DIRECT_HTTP_HEADERS,
            timeout=20,
        )
        explore_resp.raise_for_status()

        raw = explore_resp.text
        if raw.startswith(_XSSI_PREFIX):
            raw = raw[len(_XSSI_PREFIX):]
        explore_data = json.loads(raw)

        # Find the TIMESERIES widget token
        timeline_token: str | None = None
        timeline_req: Any = None
        for widget in explore_data.get("widgets", []):
            if widget.get("id") == "TIMESERIES":
                timeline_token = widget.get("token")
                timeline_req   = widget.get("request")
                break

        if not timeline_token or not timeline_req:
            log.warning("google_trends.direct.no_timeseries_widget", keywords=keywords)
            return []

        time.sleep(1.0)  # courtesy pause between explore and widget calls

        timeline_resp = self._requests.get(
            _GOOGLE_TRENDS_TIMELINE_URL,
            params={
                "hl":    "en-US",
                "tz":    0,
                "req":   json.dumps(timeline_req, separators=(",", ":")),
                "token": timeline_token,
                "tz":    0,
            },
            headers=_DIRECT_HTTP_HEADERS,
            timeout=20,
        )
        timeline_resp.raise_for_status()

        raw2 = timeline_resp.text
        if raw2.startswith(_XSSI_PREFIX):
            raw2 = raw2[len(_XSSI_PREFIX):]
        timeline_data = json.loads(raw2)

        # Parse time series into flat rows
        results: list[dict] = []
        default_data = timeline_data.get("default", {}) or {}
        for point in default_data.get("timelineData", []) or []:
            dt_str = point.get("formattedAxisTime") or point.get("formattedTime", "")
            try:
                # Various date formats from Google Trends — normalise to ISO date
                if len(dt_str) == 10 and dt_str[4] == "-":
                    dt_iso = dt_str
                else:
                    from datetime import datetime as _dt
                    dt_iso = _dt.strptime(dt_str[:10], "%b %d, %Y").date().isoformat()
            except Exception:
                dt_iso = date.today().isoformat()

            values = point.get("value", [])
            for i, kw in enumerate(keywords):
                score = float(values[i]) if i < len(values) else 0.0
                results.append({
                    "keyword_string":      kw,
                    "geography_code":      geo.upper(),
                    "signal_date":         dt_iso,
                    "signal_weight_score": score,
                    "related_topics":      None,
                    "timeframe_label":     timeframe,
                })

        log.info(
            "google_trends.direct_fetch_ok",
            keywords=keywords,
            geo=geo,
            rows=len(results),
        )
        return results


# ── Reddit client (PRAW) ───────────────────────────────────────────────────────

# Business-relevant subreddits to search by default when no explicit list provided.
_DEFAULT_SUBREDDITS = [
    "marketing", "digitalnomad", "entrepreneur", "startups",
    "SEO", "PPC", "sales", "analytics", "SaaS", "business",
]
_PRAW_POST_LIMIT = 1_000   # Reddit API hard maximum for listing endpoints


class RedditClient:
    """
    Fetch Reddit posts matching target keywords via PRAW.

    Requires environment variables (or .env entries):
      REDDIT_CLIENT_ID      — app client ID from reddit.com/prefs/apps
      REDDIT_CLIENT_SECRET  — app secret
      REDDIT_USER_AGENT     — arbitrary string identifying your app, e.g.
                              "paid-media-agent/1.0 (by u/your_username)"

    Respects the Reddit API hard limit of 1,000 posts per listing call.
    All author handles are SHA-256 hashed before storage.
    """

    def __init__(self) -> None:
        try:
            import praw  # type: ignore[import]
        except ImportError as exc:
            raise ImportError(
                "praw is required for RedditClient. "
                "Install with: pip install 'paid-media-agent[social]'"
            ) from exc

        if not settings.reddit_client_id or not settings.reddit_client_secret:
            raise RedditSetupError(
                "REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET must be set. "
                "See README for Reddit developer app setup."
            )

        self._reddit = praw.Reddit(
            client_id=settings.reddit_client_id,
            client_secret=settings.reddit_client_secret,
            user_agent=settings.reddit_user_agent or "paid-media-agent/1.0",
            ratelimit_seconds=300,
        )
        log.info("reddit.client_init", user_agent=self._reddit.config.user_agent)

    def search_subreddits(
        self,
        keywords: list[str],
        subreddits: list[str] | None = None,
        lookback_days: int = 30,
        post_limit: int = 100,
    ) -> list[dict]:
        """
        Search targeted subreddits for posts matching each keyword.

        Args:
            keywords:     Target keyword/phrase list. Each keyword triggers a
                          separate search query per subreddit.
            subreddits:   List of subreddit names (without r/ prefix).
                          Defaults to _DEFAULT_SUBREDDITS if not provided.
            lookback_days: Posts older than this many days are discarded.
            post_limit:   Max posts per keyword per subreddit search.
                          Capped at _PRAW_POST_LIMIT (1,000) — Reddit API hard limit.

        Returns:
            List of mention dicts with keys:
              post_id, keyword_matched, community_subsegment (subreddit),
              payload_text, post_title, url_reference, upvote_ratio, score,
              num_comments, engagement_score, author_handle (hashed),
              published_at (ISO str)
        """
        target_subs = subreddits if subreddits else _DEFAULT_SUBREDDITS
        capped_limit = min(post_limit, _PRAW_POST_LIMIT)
        cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)

        results: list[dict] = []
        for subreddit_name in target_subs:
            try:
                subreddit = self._reddit.subreddit(subreddit_name)
                for keyword in keywords:
                    try:
                        posts = subreddit.search(
                            query=keyword,
                            sort="new",
                            time_filter="month" if lookback_days <= 30 else "year",
                            limit=capped_limit,
                        )
                        for post in posts:
                            # Discard posts outside the lookback window
                            post_dt = datetime.fromtimestamp(
                                post.created_utc, tz=timezone.utc
                            )
                            if post_dt < cutoff:
                                continue

                            raw_text = (post.selftext or "").strip()
                            norm_text = normalize_text(raw_text or post.title or "")
                            sentiment = _score_sentiment(norm_text)
                            eng_score = (
                                float(post.score or 0) * float(post.upvote_ratio or 0)
                            )

                            results.append({
                                "post_id":            str(post.id),
                                "keyword_matched":    keyword,
                                "community_subsegment": subreddit_name,
                                "payload_text":       raw_text[:8000],    # cap field size
                                "normalized_text":    norm_text[:8000],
                                "post_title":         (post.title or "")[:500],
                                "url_reference":      f"https://reddit.com{post.permalink}",
                                "upvote_ratio":       round(float(post.upvote_ratio or 0), 4),
                                "score":              int(post.score or 0),
                                "num_comments":       int(post.num_comments or 0),
                                "engagement_score":   round(eng_score, 2),
                                "sentiment_polarity_score": sentiment,
                                "author_handle":      _hash_author(
                                    str(post.author) if post.author else None
                                ),
                                "published_at":       post_dt.isoformat(),
                            })
                    except Exception as exc:
                        log.warning(
                            "reddit.keyword_search_failed",
                            subreddit=subreddit_name,
                            keyword=keyword,
                            error=str(exc),
                        )
            except Exception as exc:
                log.warning(
                    "reddit.subreddit_access_failed",
                    subreddit=subreddit_name,
                    error=str(exc),
                )

        log.info(
            "reddit.search_complete",
            subreddits=target_subs,
            keywords=keywords,
            posts_captured=len(results),
        )
        return results


class RedditSetupError(Exception):
    """Raised when Reddit API credentials are missing or invalid."""


# ── Orchestration layer ────────────────────────────────────────────────────────

def run_social_listening(
    keywords: list[str],
    subreddits: list[str] | None = None,
    lookback_days: int = 30,
    geo_code: str = "US",
    sources: list[str] | None = None,
    run_id: str | None = None,
) -> dict:
    """
    Orchestrate Google Trends + Reddit ingestion for a keyword set.

    Writes rows to:
      social_listening_runs     (one run row)
      social_trend_signals      (one row per keyword × date × source)
      social_mentions_staging   (one row per Reddit post matched)

    Returns a summary dict:
      run_id, signals_written, mentions_written, status,
      signal_summary: {keyword: {current_avg, prior_avg, velocity_pct, trend_direction}}

    Args:
        keywords:      1–20 keyword or phrase strings.
        subreddits:    Reddit communities to search. None = _DEFAULT_SUBREDDITS.
        lookback_days: Window for "current" period data. Prior period = same length
                       immediately before the current window.
        geo_code:      ISO 3166-1 alpha-2 country code for Google Trends geo filter.
        sources:       Which sources to ingest: ["google_trends", "reddit"].
                       None = both.
        run_id:        Pre-assigned UUID. Auto-generated if None.
    """
    run_id       = run_id or str(uuid.uuid4())
    sources_list = sources or ["google_trends", "reddit"]
    now          = datetime.now(timezone.utc)

    log.info(
        "social_listening.run_start",
        run_id=run_id,
        keywords=keywords,
        sources=sources_list,
        geo_code=geo_code,
        lookback_days=lookback_days,
    )

    signals_written  = 0
    mentions_written = 0
    errors: list[str] = []

    # ── Google Trends ───────────────────────────────────────────────────────────
    trend_rows: list[dict] = []
    if "google_trends" in sources_list:
        try:
            client = GoogleTrendsClient()
            # Fetch current window
            current_signals = client.fetch_interest_over_time(
                keywords=keywords,
                geo=geo_code,
                lookback_days=lookback_days,
            )
            for row in current_signals:
                row["period_label"] = "current"
                trend_rows.append(row)

            # Fetch prior window (for MoM velocity)
            prior_signals = client.fetch_interest_over_time(
                keywords=keywords,
                geo=geo_code,
                lookback_days=lookback_days * 2,  # double window
            )
            cutoff_date = (date.today() - timedelta(days=lookback_days)).isoformat()
            for row in prior_signals:
                if row["signal_date"] < cutoff_date:
                    row["period_label"] = "prior"
                    trend_rows.append(row)

        except Exception as exc:
            errors.append(f"google_trends: {exc}")
            log.warning("social_listening.google_trends_failed", error=str(exc))

    # ── Reddit ─────────────────────────────────────────────────────────────────
    mention_rows: list[dict] = []
    if "reddit" in sources_list:
        try:
            client_reddit = RedditClient()
            raw_mentions = client_reddit.search_subreddits(
                keywords=keywords,
                subreddits=subreddits,
                lookback_days=lookback_days,
                post_limit=100,
            )
            mention_rows = raw_mentions
            # Also derive normalised signal scores from Reddit for trend_signals table
            from collections import defaultdict
            kw_counts: dict[str, int] = defaultdict(int)
            for m in mention_rows:
                kw_counts[m["keyword_matched"]] += 1

            max_count = max(kw_counts.values()) if kw_counts else 1
            today_str = date.today().isoformat()
            for kw, count in kw_counts.items():
                norm_score = round((count / max_count) * 100.0, 2)
                trend_rows.append({
                    "keyword_string":      kw,
                    "geography_code":      geo_code.upper(),
                    "signal_date":         today_str,
                    "signal_weight_score": norm_score,
                    "related_topics":      None,
                    "timeframe_label":     f"reddit_last_{lookback_days}d",
                    "platform":            "reddit_search",
                    "period_label":        "current",
                })
        except (RedditSetupError, ImportError) as exc:
            errors.append(f"reddit: {exc}")
            log.warning("social_listening.reddit_not_configured", error=str(exc))
        except Exception as exc:
            errors.append(f"reddit: {exc}")
            log.warning("social_listening.reddit_failed", error=str(exc))

    # ── Write signal rows to BQ ────────────────────────────────────────────────
    signal_bq_rows: list[dict] = []
    for row in trend_rows:
        signal_bq_rows.append({
            "signal_id":           str(uuid.uuid4()),
            "run_id":              run_id,
            "platform":            row.get("platform", "google_trends"),
            "keyword_string":      row["keyword_string"],
            "geography_code":      row.get("geography_code", geo_code),
            "signal_date":         row["signal_date"],
            "signal_weight_score": row["signal_weight_score"],
            "period_label":        row.get("period_label", "current"),
            "related_topics":      json.dumps(row["related_topics"]) if row.get("related_topics") else None,
            "timeframe_label":     row.get("timeframe_label"),
            "capture_timestamp":   now.isoformat(),
        })

    if signal_bq_rows:
        errs = bq.insert_rows("social_trend_signals", signal_bq_rows)
        signals_written = len(signal_bq_rows)
        if errs:
            log.warning("social_listening.bq_signal_insert_errors", errors=errs)

    # ── Write mention rows to BQ ───────────────────────────────────────────────
    mention_bq_rows: list[dict] = []
    for m in mention_rows:
        mention_bq_rows.append({
            "mention_id":              str(uuid.uuid4()),
            "run_id":                  run_id,
            "source_platform":         "reddit",
            "community_subsegment":    m.get("community_subsegment"),
            "keyword_matched":         m.get("keyword_matched"),
            "post_id":                 m.get("post_id"),
            "payload_text":            m.get("payload_text"),
            "normalized_text":         m.get("normalized_text"),
            "post_title":              m.get("post_title"),
            "url_reference":           m.get("url_reference"),
            "sentiment_polarity_score": m.get("sentiment_polarity_score", 0.0),
            "upvote_ratio":            m.get("upvote_ratio"),
            "score":                   m.get("score"),
            "num_comments":            m.get("num_comments"),
            "engagement_score":        m.get("engagement_score"),
            "author_handle":           m.get("author_handle"),
            "published_at":            m.get("published_at"),
            "capture_timestamp":       now.isoformat(),
        })

    if mention_bq_rows:
        errs = bq.insert_rows("social_mentions_staging", mention_bq_rows)
        mentions_written = len(mention_bq_rows)
        if errs:
            log.warning("social_listening.bq_mention_insert_errors", errors=errs)

    # ── MoM signal velocity calculation ───────────────────────────────────────
    signal_summary = _compute_mom_velocity(keywords, run_id)

    # ── Write run row ──────────────────────────────────────────────────────────
    if errors and signals_written == 0 and mentions_written == 0:
        run_status = "failed"
    elif errors:
        run_status = "partial"
    else:
        run_status = "completed"

    run_row = {
        "run_id":           run_id,
        "sources_requested": json.dumps(sources_list),
        "keywords":          json.dumps(keywords),
        "geo_codes":         json.dumps([geo_code]),
        "subreddits":        json.dumps(subreddits) if subreddits else None,
        "lookback_days":     lookback_days,
        "signals_written":   signals_written,
        "mentions_written":  mentions_written,
        "status":            run_status,
        "error_message":     "; ".join(errors) if errors else None,
        "created_by":        "analyst_agent",
        "created_at":        now.isoformat(),
    }
    bq.insert_rows("social_listening_runs", [run_row])

    log.info(
        "social_listening.run_complete",
        run_id=run_id,
        status=run_status,
        signals_written=signals_written,
        mentions_written=mentions_written,
        errors=errors,
    )

    return {
        "run_id":          run_id,
        "status":          run_status,
        "signals_written": signals_written,
        "mentions_written": mentions_written,
        "errors":          errors,
        "signal_summary":  signal_summary,
    }


def _compute_mom_velocity(keywords: list[str], run_id: str) -> dict[str, dict]:
    """
    Compute Month-over-Month signal velocity for each keyword.

    Reads from social_trend_signals where run_id matches the current run,
    grouped by keyword and period_label. Computes:
      current_avg  — mean signal_weight_score in period_label = "current"
      prior_avg    — mean signal_weight_score in period_label = "prior"
      velocity_pct — (current_avg - prior_avg) / prior_avg * 100
      trend_dir    — "rising" | "falling" | "stable" | "new" (no prior data)

    Returns dict keyed by keyword string.
    """
    if not keywords:
        return {}

    try:
        rows = bq.run_query(f"""
            SELECT
                keyword_string,
                period_label,
                AVG(signal_weight_score) AS avg_score,
                COUNT(*)                 AS data_points
            FROM {bq.table_ref('social_trend_signals')}
            WHERE run_id = '{run_id}'
              AND period_label IN ('current', 'prior')
            GROUP BY keyword_string, period_label
        """)
    except Exception as exc:
        log.warning("social_listening.mom_query_failed", error=str(exc))
        return {kw: {"current_avg": None, "prior_avg": None, "velocity_pct": None, "trend_direction": "unknown"} for kw in keywords}

    # Pivot by keyword
    from collections import defaultdict
    by_kw: dict[str, dict[str, float]] = defaultdict(dict)
    for row in rows:
        kw    = row["keyword_string"]
        label = row["period_label"]
        by_kw[kw][label] = round(float(row["avg_score"]), 2)

    summary: dict[str, dict] = {}
    for kw in keywords:
        kw_data     = by_kw.get(kw, {})
        current_avg = kw_data.get("current")
        prior_avg   = kw_data.get("prior")

        if current_avg is None:
            velocity_pct  = None
            trend_dir = "unknown"
        elif prior_avg is None or prior_avg == 0:
            velocity_pct  = None
            trend_dir = "new"      # no prior baseline — first appearance
        else:
            v = (current_avg - prior_avg) / prior_avg * 100
            velocity_pct  = round(v, 1)
            if v >= 10:
                trend_dir = "rising"
            elif v <= -10:
                trend_dir = "falling"
            else:
                trend_dir = "stable"

        summary[kw] = {
            "current_avg":      current_avg,
            "prior_avg":        prior_avg,
            "velocity_pct":     velocity_pct,
            "trend_direction":  trend_dir,
        }

    return summary
