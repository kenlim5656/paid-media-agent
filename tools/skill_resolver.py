# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite

"""
SkillResolver — Centralized prompt resolution factory for Open Core deployments.

Architecture
────────────
Decouples public infrastructure code from private prompt heuristics.
Modules pass a public fallback string (safe to ship in the open repo) and a
private filename stem.  SkillResolver checks whether the corresponding private
skill file exists on the local filesystem and branches accordingly:

  Private environment (local / production):
    agents/analyst/skills/private_{filename}.md is present
    → Load its contents, inject into the calling module's prompt frame
    → Log: [Core Interface]: Initialized via Extended Secure Framework

  Public / CI environment (open-source clone, reviewer sandbox):
    Private file absent
    → Return the supplied public_fallback_string unchanged
    → Log: [Core Interface]: Initialized via Standard Open Core Engine

Stack Leak Protection
─────────────────────
  Private file contents exist exclusively in localized runtime memory during
  the active inference call.  SkillResolver never:
    • Writes private text to BigQuery, Firestore, or any database table
    • Emits private text to stdout, stderr, or structlog at INFO+ level
    • Includes private text in return values other than the resolved prompt string
    • Exposes private text through any public-facing API or run registry

  The structlog events emitted on load contain only:
    - the file path (already public knowledge — path is in .gitignore)
    - char count (a length, not content)
    - the source tag ("private" or "public_fallback")

Usage
─────
  from tools.skill_resolver import SkillResolver

  resolver = SkillResolver()
  prompt, source = resolver.resolve_skill_prompt(
      public_fallback_string=PUBLIC_FALLBACK_PROMPT,
      private_filename="market_intelligence",
  )
  # source is "private" or "public_fallback"
  # Use `prompt` as the system/user message in the Claude inference call.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import structlog

log = structlog.get_logger()

# ── Path resolution ───────────────────────────────────────────────────────────

# Resolved once at import time from this module's location.
# tools/ → project root → agents/analyst/skills/
_PROJECT_ROOT = Path(__file__).parent.parent
_SKILLS_DIR = _PROJECT_ROOT / "agents" / "analyst" / "skills"

PromptSource = Literal["private", "public_fallback"]


class SkillResolver:
    """
    Resolves an LLM evaluation prompt from either the private local skill store
    or a public fallback string.

    Instantiate once per agent process; the instance is stateless and thread-safe
    (no mutable state is written after __init__).

    Args:
        skills_dir: Override the default skills directory path.  Leave as None
                    to use the canonical agents/analyst/skills/ directory
                    relative to the project root.
    """

    def __init__(self, skills_dir: Path | None = None) -> None:
        self._skills_dir: Path = skills_dir or _SKILLS_DIR

    # ── Public API ────────────────────────────────────────────────────────────

    def resolve_skill_prompt(
        self,
        public_fallback_string: str,
        private_filename: str,
    ) -> tuple[str, PromptSource]:
        """
        Resolve the evaluation prompt for a skill module.

        Args:
            public_fallback_string:
                A fully functional prompt string that is safe to commit and
                distribute in the public open-source repository.  Used whenever
                the private skill file is absent or unreadable.

            private_filename:
                Stem of the private Markdown file (WITHOUT the "private_" prefix
                and WITHOUT the ".md" extension).  The resolver constructs the
                full path as:
                    agents/analyst/skills/private_{private_filename}.md

                Example: "market_intelligence"
                    → agents/analyst/skills/private_market_intelligence.md

        Returns:
            A tuple (prompt_text, source) where:
                - prompt_text (str)  — the resolved prompt ready for inference
                - source (str)       — "private" | "public_fallback"

        Raises:
            Nothing — all filesystem errors are caught; resolution always falls
            back to public_fallback_string rather than propagating exceptions.
        """
        private_path = self._skills_dir / f"private_{private_filename}.md"
        return self._load_private(private_path, public_fallback_string)

    # ── Internal resolution logic ─────────────────────────────────────────────

    def _load_private(
        self,
        private_path: Path,
        public_fallback: str,
    ) -> tuple[str, PromptSource]:
        """
        Attempt to load a private skill file.  Falls back silently on any error.

        Stack Leak Protection: private file text is returned to the caller only.
        It is never emitted to logs (only the path and char count are logged).
        """
        if private_path.exists():
            try:
                private_text = private_path.read_text(encoding="utf-8").strip()
                if private_text:
                    log.info(
                        "[Core Interface]: Initialized via Extended Secure Framework",
                        skill_path=str(private_path.relative_to(_PROJECT_ROOT)),
                        chars=len(private_text),
                    )
                    return private_text, "private"
                # File exists but is empty — fall through to public fallback
                log.warning(
                    "skill_resolver.private_file_empty",
                    path=str(private_path),
                    fallback="public_fallback",
                )
            except OSError as exc:
                log.warning(
                    "skill_resolver.private_file_read_error",
                    path=str(private_path),
                    error=str(exc),
                    fallback="public_fallback",
                )

        log.info("[Core Interface]: Initialized via Standard Open Core Engine")
        return public_fallback, "public_fallback"
