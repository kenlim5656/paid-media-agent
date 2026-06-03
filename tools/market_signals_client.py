# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
Market Signals Client — competitive intelligence ingestion engine (Task 36).

Harvests publicly accessible competitor web content (landing pages, press releases,
text dumps), normalises raw text through the Task 25 pipeline, and passes the
result through a Claude inference call to extract structured competitive
messaging profiles.

Architecture
────────────
  Source URLs / text dumps
          │
          ▼
  MarketSignalsClient._fetch_url_text()   (httpx, rate-limited, 20s timeout)
          │
          ▼
  normalize_text()                         ← imported directly from Task 25
  [data URI → tracking pixel → HTML tag → emoji → whitespace collapse]
          │
          ▼
  market_signals_staging (BigQuery)
          │
          ▼
  MarketSignalsClient._run_inference()     (Claude sub-call)
  ┌──────────────────────────────────────────────────────────┐
  │  Prompt resolution (Stealth Execution Gateway):          │
  │    1. Check agents/analyst/skills/private_market_        │
  │       intelligence.md  → load if present                 │
  │    2. Fallback: generic semantic summarization prompt     │
  └──────────────────────────────────────────────────────────┘
          │
          ▼
  competitor_messaging_vectors (BigQuery)

Public API
──────────
  MarketSignalsClient.run_extraction(source_urls, competitor_name, category)
    → full pipeline: fetch → normalize → store staging → infer → store vectors
    → {run_id, status, signal_count, vector, prompt_source}

  get_competitor_context(competitor_name, category, limit)
    → BQ query of competitor_messaging_vectors for Task 26 copy generation context

  list_tracked_competitors(limit)
    → distinct competitor names + latest run timestamps for discovery

Private skills file
───────────────────
  Path:    agents/analyst/skills/private_market_intelligence.md
  Purpose: Proprietary evaluation heuristics for the LLM parsing layer.
           Allows practitioners to encode their own competitive intelligence
           playbook, scoring rubrics, and market-specific terminology.
  Access:  Read-only at inference time. Never persisted to BigQuery.
  Git:     Listed in .gitignore and .claudeignore — never committed.
           If file is absent, the client falls back to the generic prompt
           with zero service degradation.

Task 26 integration
───────────────────
  Call get_competitor_context() before generate_creative_campaign_brief()
  to inject counter-positioning hooks into the copy generation prompt.
  The Analyst agent tool (ingest_and_analyze_market_signals) exposes this
  helper under the 'competitor_context_for_copy' key in its return payload.
"""
from __future__ import annotations

import json
import re
import time
import uuid
from collections import Counter
from datetime import date, datetime, timezone
from typing import Any

import structlog

import anthropic
from config import settings
from tools import bigquery_client as bq
from tools.skill_resolver import SkillResolver
from tools.social_listening_client import normalize_text  # Task 25 — direct import

# ── Skill resolver singleton ──────────────────────────────────────────────────
_skill_resolver = SkillResolver()

log = structlog.get_logger()

# ── Module-level constants ──────────────────────────────────────────────────────

# HTTP fetch settings
_FETCH_TIMEOUT_S       = 20        # per-URL HTTP timeout
_INTER_REQUEST_DELAY_S = 2.0       # politeness delay between URL fetches
_MAX_PAYLOAD_CHARS     = 50_000    # BQ storage cap on raw_text_payload
_MAX_URLS_PER_RUN      = 25        # safety cap on URLs per run

# Stop words excluded from keyword frequency counts
_STOP_WORDS = frozenset({
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "up", "as", "is", "are", "was", "were",
    "be", "been", "being", "have", "has", "had", "do", "does", "did",
    "will", "would", "could", "should", "may", "might", "shall", "can",
    "not", "no", "nor", "so", "yet", "both", "either", "whether", "this",
    "that", "these", "those", "we", "you", "it", "its", "our", "your",
    "their", "them", "they", "he", "she", "who", "which", "what", "how",
    "all", "each", "every", "any", "more", "most", "other", "into",
    "than", "then", "when", "where", "while", "about", "after", "before",
    "through", "during", "without", "within", "between", "across", "over",
    "under", "just", "also", "even", "if", "because", "because", "here",
    "there", "very", "only", "get", "use", "new", "one", "two", "make",
})

_RE_WORD_TOKENIZE = re.compile(r"\b[a-zA-Z][a-zA-Z0-9\-]{2,}\b")


# ── Prompt resolution via SkillResolver ──────────────────────────────────────

# Public fallback — fully functional for open-source deployments.
# Extracts core competitive signals and produces a structured JSON profile.
# For extended heuristics (proprietary scoring, market-specific terminology,
# counter-positioning playbook), place private_market_intelligence.md at:
#   agents/analyst/skills/private_market_intelligence.md
# That file is gitignored and loaded automatically when present.
_PUBLIC_FALLBACK_PROMPT = """
You are a competitive intelligence analyst. Analyze the competitor's marketing content
and extract a structured profile. Output ONLY valid JSON — no preamble, no markdown
fences, no explanation. Start with '{' and end with '}'.

Required fields:
{
  "core_value_prop": "<one sentence>",
  "observed_positioning_angle": "<price_leadership | quality_premium | speed_efficiency | ease_simplicity | category_creation | vertical_specialist | trust_security | mixed>",
  "inferred_target_audience": "<buyer persona description>",
  "sentiment_tone": "<authoritative | conversational | aggressive | aspirational | technical | friendly | neutral>",
  "primary_keywords_detected": ["<top 15 strategic terms, no stop words>"],
  "key_themes": [
    {"theme": "<name>", "frequency": <int>, "example_phrase": "<verbatim quote>"}
  ],
  "messaging_pillars": [
    {"pillar": "<name>", "supporting_claims": ["<claim>"]}
  ],
  "cta_patterns": ["<CTA phrase>"],
  "counter_positioning_hooks": [
    {"angle": "<angle>", "hook": "<ad hook>", "rationale": "<why this gaps their positioning>"}
  ],
  "confidence_score": <0.0–1.0>
}

Rules: extract only what is demonstrably present; use null for fields with insufficient data.
""".strip()


def _resolve_evaluation_prompt() -> tuple[str, str]:
    """
    Delegate prompt resolution to SkillResolver.

    Returns (prompt_text, source) where source is "private" or "public_fallback".
    """
    return _skill_resolver.resolve_skill_prompt(
        public_fallback_string=_PUBLIC_FALLBACK_PROMPT,
        private_filename="market_intelligence",
    )


# ── Keyword frequency helper ─────────────────────────────────────────────────

def _extract_top_keywords(text: str, top_n: int = 20) -> list[dict]:
    """
    Extract top-N most frequent content words from normalized text.
    Excludes stop words and tokens shorter than 3 characters.
    Returns list of {'keyword': str, 'count': int} sorted by frequency desc.
    """
    tokens = [
        t.lower()
        for t in _RE_WORD_TOKENIZE.findall(text)
        if t.lower() not in _STOP_WORDS
    ]
    counter = Counter(tokens)
    return [
        {"keyword": kw, "count": cnt}
        for kw, cnt in counter.most_common(top_n)
    ]



# ── Main client class ─────────────────────────────────────────────────────────

class MarketSignalsClient:
    """
    Competitive intelligence ingestion and inference engine.

    Lifecycle of one extraction run:
      1. _ingest_urls()       — fetch + normalize each URL; write to market_signals_staging
      2. _run_inference()     — pass combined normalized text to Claude with resolved prompt
      3. result logging       — write run row to market_signals_runs; update staging statuses
    """

    def __init__(self) -> None:
        self._run_id: str = str(uuid.uuid4())

    # ── Public API ──────────────────────────────────────────────────────────────

    def run_extraction(
        self,
        source_urls: list[str],
        competitor_name: str,
        category: str | None = None,
    ) -> dict:
        """
        Execute a full market signals extraction pipeline.

        Args:
            source_urls:      List of publicly accessible HTTP/HTTPS URLs to analyze.
                              May also include "text://<payload>" scheme strings to pass
                              raw text directly without an HTTP fetch.
            competitor_name:  Canonical competitor label (used in BQ and reports).
            category:         Optional market category for prompt scoping,
                              e.g. "B2B SaaS", "ecommerce", "marketing automation".

        Returns:
            {
              ok             — True if at least one signal was fetched and vector written
              run_id         — UUID for this run (links BQ tables)
              signal_count   — rows written to market_signals_staging
              vector         — the structured inference dict (from competitor_messaging_vectors)
              prompt_source  — "private" | "public_fallback"
              errors         — list of per-URL error strings
            }
        """
        if not source_urls:
            return {
                "ok": False,
                "run_id": self._run_id,
                "error": "source_urls is empty — provide at least one URL or text:// payload.",
            }

        # Cap URLs to prevent runaway costs
        if len(source_urls) > _MAX_URLS_PER_RUN:
            log.warning(
                "market_signals.urls.capped",
                submitted=len(source_urls),
                cap=_MAX_URLS_PER_RUN,
            )
            source_urls = source_urls[:_MAX_URLS_PER_RUN]

        # ── Step 1: Resolve evaluation prompt ──────────────────────────────
        evaluation_prompt, prompt_source = _resolve_evaluation_prompt()

        # ── Step 2: Fetch + normalize URLs ─────────────────────────────────
        signals, errors = self._ingest_urls(
            urls=source_urls,
            competitor_name=competitor_name,
        )

        if not signals:
            self._write_run_row(
                competitor_name=competitor_name,
                source_urls=source_urls,
                category=category,
                prompt_source=prompt_source,
                urls_submitted=len(source_urls),
                urls_ok=0,
                urls_failed=len(source_urls),
                signals_written=0,
                vectors_written=0,
                status="failed",
                error_message=f"All {len(source_urls)} URLs failed: {'; '.join(errors[:3])}",
            )
            return {
                "ok":           False,
                "run_id":       self._run_id,
                "signal_count": 0,
                "vector":       None,
                "prompt_source": prompt_source,
                "errors":       errors,
            }

        # ── Step 3: Run Claude inference ────────────────────────────────────
        combined_text = "\n\n---\n\n".join(
            f"[Source: {s['source_url']}]\n{s['raw_text_payload']}"
            for s in signals
        )

        vector, inference_error = self._run_inference(
            competitor_name=competitor_name,
            combined_text=combined_text,
            evaluation_prompt=evaluation_prompt,
            category=category,
            signal_ids=[s["signal_id"] for s in signals],
        )

        vectors_written = 1 if vector else 0

        # ── Step 4: Update staging statuses ────────────────────────────────
        new_status = "inferred" if vector else "failed"
        for s in signals:
            self._update_signal_status(s["signal_id"], new_status)

        # ── Step 5: Write run log row ───────────────────────────────────────
        overall_status = (
            "completed" if vector and not errors
            else "partial" if vector
            else "failed"
        )
        self._write_run_row(
            competitor_name=competitor_name,
            source_urls=source_urls,
            category=category,
            prompt_source=prompt_source,
            urls_submitted=len(source_urls),
            urls_ok=len(signals),
            urls_failed=len(errors),
            signals_written=len(signals),
            vectors_written=vectors_written,
            status=overall_status,
            error_message=inference_error,
        )

        log.info(
            "market_signals.run_complete",
            run_id=self._run_id,
            competitor=competitor_name,
            signals=len(signals),
            errors=len(errors),
            vector_written=vectors_written > 0,
            prompt_source=prompt_source,
        )

        return {
            "ok":            vectors_written > 0,
            "run_id":        self._run_id,
            "signal_count":  len(signals),
            "vector":        vector,
            "prompt_source": prompt_source,
            "errors":        errors,
            "status":        overall_status,
        }

    # ── URL ingestion ────────────────────────────────────────────────────────

    def _ingest_urls(
        self,
        urls: list[str],
        competitor_name: str,
    ) -> tuple[list[dict], list[str]]:
        """
        Fetch, normalize, and write each URL to market_signals_staging.

        Supports two URL schemes:
          http:// / https://   — standard HTTP fetch via httpx
          text://              — raw text payload (no HTTP fetch; stripped of scheme prefix)

        Rate-limited at _INTER_REQUEST_DELAY_S seconds between HTTP requests.
        Returns: (signal_rows_written, error_strings)
        """
        signals: list[dict] = []
        errors: list[str]   = []
        now = datetime.now(timezone.utc)

        for i, url in enumerate(urls):
            if url.startswith("text://"):
                raw_text = url[len("text://"):]
                normalized = normalize_text(raw_text)
                http_status  = None
                content_type = "text/plain"
                fetch_error  = None
            else:
                # Polite rate limit between HTTP fetches
                if i > 0:
                    time.sleep(_INTER_REQUEST_DELAY_S)
                raw_text, http_status, content_type, fetch_error = self._fetch_url_text(url)
                normalized = normalize_text(raw_text) if raw_text else ""

            if fetch_error and not normalized:
                errors.append(f"{url}: {fetch_error}")
                continue

            if not normalized.strip():
                errors.append(f"{url}: normalized text was empty after scrubbing")
                continue

            # Derive source type heuristic from URL / content
            source_type = _classify_source_type(url, content_type)

            # Keyword frequency for staging analytics
            top_kws = _extract_top_keywords(normalized, top_n=20)

            signal_id = str(uuid.uuid4())
            row = {
                "signal_id":          signal_id,
                "run_id":             self._run_id,
                "source_url":         url[:2048],   # BQ STRING safety cap
                "source_type":        source_type,
                "http_status_code":   http_status,
                "content_type":       content_type,
                "raw_text_payload":   normalized[:_MAX_PAYLOAD_CHARS],
                "estimated_word_count": len(normalized.split()),
                "text_language_hint": None,          # language detection is optional
                "top_keywords_json":  json.dumps(top_kws),
                "status":             "raw",
                "error_message":      fetch_error,
                "capture_date":       now.date().isoformat(),
                "capture_timestamp":  now.isoformat(),
            }

            try:
                bq.insert_rows("market_signals_staging", [row])
                signals.append(row)
                log.info(
                    "market_signals.signal_written",
                    signal_id=signal_id,
                    url=url[:80],
                    word_count=row["estimated_word_count"],
                )
            except Exception as exc:
                errors.append(f"{url}: BQ write failed — {exc}")

        return signals, errors

    # ── HTTP fetch ───────────────────────────────────────────────────────────

    def _fetch_url_text(self, url: str) -> tuple[str, int | None, str | None, str | None]:
        """
        Fetch the raw body text of a URL.

        Returns: (raw_text, http_status_code, content_type, error_message)
          raw_text may be empty string on error; error_message is None on success.

        Uses a compliant User-Agent string so the request is identifiable.
        Follows redirects (up to 5). Decodes UTF-8 with latin-1 fallback.
        """
        try:
            import httpx
        except ImportError:
            return "", None, None, (
                "httpx is not installed. Install with: pip install httpx"
            )

        headers = {
            "User-Agent": (
                "paid-media-agent/1.0 market-signals-client "
                "(competitive intelligence; +https://github.com/arcticgreyy/paid-media-suite)"
            ),
            "Accept":          "text/html,application/xhtml+xml,text/plain;q=0.9",
            "Accept-Language": "en-US,en;q=0.5",
        }

        try:
            resp = httpx.get(
                url,
                headers=headers,
                timeout=_FETCH_TIMEOUT_S,
                follow_redirects=True,
                max_redirects=5,
            )
            content_type = resp.headers.get("content-type", "")
            # Decode body — httpx handles charset detection via apparent_encoding
            try:
                text = resp.text
            except Exception:
                text = resp.content.decode("utf-8", errors="replace")

            if resp.status_code >= 400:
                return (
                    text,
                    resp.status_code,
                    content_type,
                    f"HTTP {resp.status_code}",
                )
            return text, resp.status_code, content_type, None

        except httpx.TimeoutException:
            return "", None, None, f"Timeout after {_FETCH_TIMEOUT_S}s"
        except httpx.TooManyRedirects:
            return "", None, None, "Too many redirects (>5)"
        except httpx.RequestError as exc:
            return "", None, None, f"Request error: {exc}"
        except Exception as exc:
            return "", None, None, f"Unexpected fetch error: {exc}"

    # ── Claude inference ─────────────────────────────────────────────────────

    def _run_inference(
        self,
        competitor_name: str,
        combined_text: str,
        evaluation_prompt: str,
        category: str | None,
        signal_ids: list[str],
    ) -> tuple[dict | None, str | None]:
        """
        Pass normalized competitor text through Claude to extract a structured
        messaging profile. Writes the result to competitor_messaging_vectors.

        The user prompt wraps the evaluation_prompt (private or public fallback)
        around the combined source text. The system prompt instructs the model
        to output JSON only.

        Returns: (vector_dict, error_message_or_None)
        """
        category_note = f"\nMarket category context: {category}" if category else ""
        word_count    = len(combined_text.split())

        user_message = (
            f"Competitor: {competitor_name}{category_note}\n"
            f"Total content analyzed: ~{word_count:,} words from "
            f"{len(signal_ids)} source(s)\n\n"
            f"=== EVALUATION INSTRUCTIONS ===\n{evaluation_prompt}\n\n"
            f"=== COMPETITOR CONTENT ===\n{combined_text[:40_000]}"
            # Cap at 40k chars; model context handles the rest
        )

        try:
            client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
            response = client.messages.create(
                model=settings.claude_model,
                max_tokens=4096,
                system=(
                    "You are a competitive intelligence analyst. "
                    "Output ONLY a valid JSON object. No preamble, no markdown fences. "
                    "Start with '{' and end with '}'."
                ),
                messages=[{"role": "user", "content": user_message}],
            )
            raw_json = response.content[0].text.strip()

            # Strip accidental markdown fences
            if raw_json.startswith("```"):
                raw_json = raw_json.split("\n", 1)[1]
                raw_json = raw_json.rsplit("```", 1)[0].strip()

            parsed = json.loads(raw_json)

        except json.JSONDecodeError as exc:
            log.error("market_signals.inference.json_parse_failed", error=str(exc))
            return None, f"JSON parse failed: {exc}"
        except Exception as exc:
            log.error("market_signals.inference.failed", error=str(exc))
            return None, f"Inference call failed: {exc}"

        # ── Write to competitor_messaging_vectors ────────────────────────────
        now      = datetime.now(timezone.utc)
        vector_id = str(uuid.uuid4())

        vector_row = {
            "vector_id":                  vector_id,
            "run_id":                     self._run_id,
            "signal_ids":                 json.dumps(signal_ids),
            "competitor_name":            competitor_name,
            "competitor_domain":          _extract_domain_from_competitor(competitor_name),
            "category":                   category,
            "core_value_prop":            parsed.get("core_value_prop"),
            "observed_positioning_angle": parsed.get("observed_positioning_angle"),
            "inferred_target_audience":   parsed.get("inferred_target_audience"),
            "sentiment_tone":             parsed.get("sentiment_tone"),
            "primary_keywords_detected":  json.dumps(parsed.get("primary_keywords_detected", [])),
            "key_themes_json":            json.dumps(parsed.get("key_themes", [])),
            "messaging_pillars_json":     json.dumps(parsed.get("messaging_pillars", [])),
            "cta_patterns_json":          json.dumps(parsed.get("cta_patterns", [])),
            "counter_positioning_hooks_json": json.dumps(
                parsed.get("counter_positioning_hooks", [])
            ),
            "source_url_count":           len(signal_ids),
            "total_word_count":           word_count,
            "evaluation_prompt_source":   "private" if _PRIVATE_INTEL_PATH.exists() else "public_fallback",
            "raw_inference_json":         json.dumps(parsed),
            "capture_timestamp":          now.isoformat(),
        }

        try:
            bq.insert_rows("competitor_messaging_vectors", [vector_row])
            log.info(
                "market_signals.vector_written",
                vector_id=vector_id,
                competitor=competitor_name,
                positioning=parsed.get("observed_positioning_angle"),
            )
        except Exception as exc:
            log.error("market_signals.vector_bq_write_failed", error=str(exc))
            return None, f"BQ vector write failed: {exc}"

        return vector_row, None

    # ── BQ helpers ───────────────────────────────────────────────────────────

    def _write_run_row(
        self,
        competitor_name: str,
        source_urls: list[str],
        category: str | None,
        prompt_source: str,
        urls_submitted: int,
        urls_ok: int,
        urls_failed: int,
        signals_written: int,
        vectors_written: int,
        status: str,
        error_message: str | None,
    ) -> None:
        row = {
            "run_id":                  self._run_id,
            "source_type":             "url_batch",
            "competitor_name":         competitor_name,
            "target_competitor_domain": _extract_domain_from_competitor(competitor_name),
            "source_urls":             json.dumps(source_urls),
            "category":                category,
            "evaluation_prompt_source": prompt_source,
            "urls_submitted":          urls_submitted,
            "urls_fetched_ok":         urls_ok,
            "urls_failed":             urls_failed,
            "signals_written":         signals_written,
            "vectors_written":         vectors_written,
            "status":                  status,
            "error_message":           error_message,
            "created_by":              "analyst_agent",
            "created_at":              datetime.now(timezone.utc).isoformat(),
        }
        try:
            bq.insert_rows("market_signals_runs", [row])
        except Exception as exc:
            log.warning("market_signals.run_row_write_failed", error=str(exc))

    def _update_signal_status(self, signal_id: str, status: str) -> None:
        try:
            bq.run_dml(f"""
                UPDATE {bq.table_ref('market_signals_staging')}
                SET status = '{status}'
                WHERE signal_id = '{signal_id}'
            """)
        except Exception as exc:
            log.warning("market_signals.staging_status_update_failed", error=str(exc))


# ── Module-level query helpers ───────────────────────────────────────────────

def get_competitor_context(
    competitor_name: str | None = None,
    category: str | None = None,
    limit: int = 5,
) -> list[dict]:
    """
    Query competitor_messaging_vectors for Task 26 copy generation context.

    Called by the Task 26 Copy Assistant (generate_creative_campaign_brief)
    before building the copy prompt to inject counter-positioning hooks.

    Args:
        competitor_name: Filter to a specific competitor (exact match, case-insensitive).
                         If None, returns the most recent vectors across all competitors.
        category:        Optional category filter (e.g. "B2B SaaS").
        limit:           Max rows to return (default 5 — sufficient for prompt injection).

    Returns:
        List of vector dicts with keys matching competitor_messaging_vectors columns.
        Empty list if no vectors exist or BQ is unavailable.
    """
    filters = []
    if competitor_name:
        safe = competitor_name.replace("'", "\\'")
        filters.append(f"LOWER(competitor_name) = LOWER('{safe}')")
    if category:
        safe_cat = category.replace("'", "\\'")
        filters.append(f"LOWER(category) = LOWER('{safe_cat}')")

    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    sql = f"""
        SELECT
            vector_id,
            competitor_name,
            competitor_domain,
            category,
            core_value_prop,
            observed_positioning_angle,
            inferred_target_audience,
            sentiment_tone,
            primary_keywords_detected,
            key_themes_json,
            messaging_pillars_json,
            cta_patterns_json,
            counter_positioning_hooks_json,
            capture_timestamp
        FROM {bq.table_ref('competitor_messaging_vectors')}
        {where_clause}
        ORDER BY capture_timestamp DESC
        LIMIT {int(limit)}
    """
    try:
        return bq.run_query(sql)
    except Exception as exc:
        log.warning("market_signals.get_competitor_context_failed", error=str(exc))
        return []


def list_tracked_competitors(limit: int = 20) -> list[dict]:
    """
    Return distinct competitor names with latest analysis timestamp.
    Used by the Analyst agent for discovery and the Markdown summary.
    """
    sql = f"""
        SELECT
            competitor_name,
            MAX(capture_timestamp) AS latest_analysis,
            COUNT(*)               AS vector_count
        FROM {bq.table_ref('competitor_messaging_vectors')}
        GROUP BY competitor_name
        ORDER BY latest_analysis DESC
        LIMIT {int(limit)}
    """
    try:
        return bq.run_query(sql)
    except Exception as exc:
        log.warning("market_signals.list_competitors_failed", error=str(exc))
        return []


def format_competitor_context_for_copy(vectors: list[dict]) -> str:
    """
    Format competitor vectors into a structured text block for injection
    into the Task 26 copy generation prompt as competitive context.

    Output is designed to be appended to the generation prompt's context section.
    """
    if not vectors:
        return ""

    lines = ["=== Competitor Intelligence Context (for counter-positioning) ==="]
    for v in vectors:
        name     = v.get("competitor_name", "Unknown")
        prop     = v.get("core_value_prop") or "n/a"
        angle    = v.get("observed_positioning_angle") or "n/a"
        audience = v.get("inferred_target_audience") or "n/a"
        tone     = v.get("sentiment_tone") or "n/a"

        # Parse JSON fields safely
        keywords = []
        try:
            kws = v.get("primary_keywords_detected")
            if isinstance(kws, str):
                kws = json.loads(kws)
            keywords = kws[:8] if kws else []
        except Exception:
            pass

        hooks = []
        try:
            raw_hooks = v.get("counter_positioning_hooks_json")
            if isinstance(raw_hooks, str):
                raw_hooks = json.loads(raw_hooks)
            hooks = raw_hooks[:3] if raw_hooks else []
        except Exception:
            pass

        lines += [
            f"\n[Competitor: {name}]",
            f"  Value prop:  {prop}",
            f"  Positioning: {angle} | Tone: {tone}",
            f"  Targets:     {audience}",
            f"  Key terms:   {', '.join(keywords)}",
        ]
        if hooks:
            lines.append("  Counter-positioning hooks:")
            for h in hooks:
                lines.append(f"    • {h.get('angle', '')}: {h.get('hook', '')}")

    lines.append("")
    return "\n".join(lines)


# ── Utility functions ────────────────────────────────────────────────────────

def _classify_source_type(url: str, content_type: str | None) -> str:
    """Heuristic classification of URL content type for staging metadata."""
    url_lower = url.lower()
    if url.startswith("text://"):
        return "text_dump"
    if any(kw in url_lower for kw in ["press-release", "press_release", "newsroom", "news/"]):
        return "press_release"
    if any(kw in url_lower for kw in ["blog", "/post/", "/article/", "insights/"]):
        return "blog_post"
    if content_type and "text/plain" in content_type:
        return "text_dump"
    return "landing_page"


def _extract_domain_from_competitor(competitor_name: str) -> str | None:
    """
    Best-effort extraction of a domain-like string from a competitor name.
    Returns None if no domain pattern is detectable.
    e.g. "Acme Corp (acme.com)" → "acme.com"
         "acme.com" → "acme.com"
         "Acme Corp" → None
    """
    # Try to find an explicit domain in parentheses
    m = re.search(r'\(([a-z0-9\-]+\.[a-z]{2,})\)', competitor_name, re.IGNORECASE)
    if m:
        return m.group(1).lower()
    # The whole string looks like a domain
    if re.match(r'^[a-z0-9\-]+\.[a-z]{2,}', competitor_name, re.IGNORECASE):
        return competitor_name.lower().split("/")[0]
    return None
