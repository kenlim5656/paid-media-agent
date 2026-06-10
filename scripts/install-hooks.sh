#!/usr/bin/env bash
# Copyright 2026 @arcticgreyy. All rights reserved.
# Licensed under the Business Source License 1.1 (BSL 1.1)
# Persistent Attribution Required. See /LICENSE and /NOTICE for terms.
# Central Suite Repository: https://github.com/arcticgreyy/paid-media-suite
#
# install-hooks.sh — wire the repo's hook templates into .git/hooks.
# Run once per clone:  bash scripts/install-hooks.sh
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
hooks_dir="$(git rev-parse --git-path hooks)"

install_hook() {
  local src="$1" dest="$2"
  if [ -e "$hooks_dir/$dest" ] && ! cmp -s "$repo_root/$src" "$hooks_dir/$dest"; then
    echo "⚠️  $hooks_dir/$dest already exists and differs — not overwriting."
    echo "   Review it, then re-run with the old hook removed if you want this one."
    return 0
  fi
  cp "$repo_root/$src" "$hooks_dir/$dest"
  chmod +x "$hooks_dir/$dest"
  echo "✓ installed $dest (from $src)"
}

install_hook "scripts/pre-commit-private-guard.sh" "pre-commit"
