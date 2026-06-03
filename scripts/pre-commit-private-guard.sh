#!/usr/bin/env bash
# pre-commit-private-guard.sh
#
# Blocks a git commit if any staged file matches private-asset naming patterns.
#
# Installation (run once per clone):
#   cp scripts/pre-commit-private-guard.sh .git/hooks/pre-commit
#   chmod +x .git/hooks/pre-commit
#
# This is a template — copy it to .git/hooks/pre-commit on each developer workstation.
# The .git/ directory is not tracked, so this must be installed manually or via
# a post-checkout script / onboarding guide.

set -euo pipefail

# ── Patterns that must never reach git staging ────────────────────────────────
BLOCKED_PATTERNS=(
    "private_"          # any file whose path contains the string "private_"
    "_private.json"     # JSON config files flagged as private
    "private_config/"   # entire private config directories
)

# ── Check staged files ────────────────────────────────────────────────────────
staged_files=$(git diff --cached --name-only 2>/dev/null || true)

if [[ -z "$staged_files" ]]; then
    exit 0
fi

violations=()

while IFS= read -r file; do
    for pattern in "${BLOCKED_PATTERNS[@]}"; do
        if echo "$file" | grep -q "$pattern"; then
            violations+=("$file  [matched: '$pattern']")
            break
        fi
    done
done <<< "$staged_files"

# ── Report and block ──────────────────────────────────────────────────────────
if [[ ${#violations[@]} -gt 0 ]]; then
    echo ""
    echo "╔═══════════════════════════════════════════════════════════════════╗"
    echo "║  COMMIT BLOCKED — private asset detected in staging index         ║"
    echo "╚═══════════════════════════════════════════════════════════════════╝"
    echo ""
    echo "The following staged files match a private-asset naming pattern"
    echo "and must NOT be committed to version control:"
    echo ""
    for v in "${violations[@]}"; do
        echo "  ✗  $v"
    done
    echo ""
    echo "To fix:"
    echo "  git reset HEAD <file>   — unstage the file"
    echo "  Confirm the file is listed in .gitignore before retrying."
    echo ""
    exit 1
fi

exit 0
