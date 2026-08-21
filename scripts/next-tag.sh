#!/usr/bin/env bash
# Print the next release tag for the module repo in $1 (default: cwd).
#
# Exists because a hand-rolled sweep used `git tag -l 'v0.1.*'` to find the
# current version, and 14 of 18 modules still carry an abandoned v0.1.x series
# with hundreds of tags in it. common-module's live series is v0.9.x, so the
# glob happily reported v0.1.238 and the sweep tagged v0.1.239 — a release
# numbered *below* the version already in the catalog, with an image published
# to match. Nothing errored; the tags simply went to the wrong place.
#
# The rule: never assume a series. Take every well-formed vN.N.N tag, sort them
# by version, and increment the highest. That is the only reading that survives
# a module whose minor moved on and left its old series behind.
set -euo pipefail

repo="${1:-.}"
cd "$repo"

latest=$(git tag -l 'v*' | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | sort -V | tail -1)

if [ -z "$latest" ]; then
  # A module with no release yet. v0.0.1 rather than guessing a minor.
  echo "v0.0.1"
  exit 0
fi

echo "$latest" | awk -F. '{printf "%s.%s.%d\n", $1, $2, $3+1}'
