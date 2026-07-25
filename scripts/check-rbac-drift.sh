#!/usr/bin/env bash
# check-rbac-drift.sh — guard against the RBAC drift that let pod_logs_get ship
# without `pods/log` access.
#
# The self-hosted install renders each module's ClusterRole from that module's
# values.yaml `rbac:` overlay in this index repo. The authoritative declaration,
# though, lives in the module's Go code (registry.SetRequirements) and is emitted
# by the module image as `tools rbac-values` (SDK >= v0.13.35). This script fails
# if a committed overlay does not match what the image declares, so the two can
# never silently diverge again.
#
# Modules whose image predates the `tools rbac-values` command are skipped (the
# gate is progressive: it starts enforcing a module the release after it adopts
# SDK v0.13.35+). Set STRICT=1 to fail instead of skip on a missing command.
set -uo pipefail

cd "$(dirname "$0")/.." || exit 2
fail=0
checked=0

for mf in */module.yaml; do
  [ -f "$mf" ] || continue
  dir=$(dirname "$mf")
  img=$(grep -Eo 'image:[[:space:]]*[^[:space:]]+' "$mf" | head -1 | awk '{print $2}')
  if [ -z "$img" ]; then
    echo "· $dir: no image in module.yaml — skipped"
    continue
  fi

  # The image CMD is "/manager run"; override the entrypoint to the binary so
  # `tools rbac-values` runs instead of being treated as the executable name.
  declared=$(docker run --rm --entrypoint /manager "$img" tools rbac-values 2>/dev/null)
  rc=$?
  # A v0.13.35+ image emits nothing (module declares no RBAC) or a block that
  # starts with `rbac:`. Older images lack the subcommand — cobra prints the
  # `tools` help to stdout (exit 0) or the run fails. Detect both and skip, so
  # a pre-v0.13.35 image is never mistaken for one that "declares" help text.
  first=$(printf '%s' "$declared" | head -1)
  if [ $rc -ne 0 ] || { [ -n "$declared" ] && [ "$first" != "rbac:" ]; }; then
    if [ "${STRICT:-0}" = "1" ]; then
      echo "✗ $dir: $img has no 'tools rbac-values' and STRICT=1"
      fail=1
    else
      echo "· $dir: $img has no 'tools rbac-values' (pre-v0.13.35 SDK) — skipped"
    fi
    continue
  fi

  overlay=""
  [ -f "$dir/values.yaml" ] && overlay=$(grep -v '^[[:space:]]*#' "$dir/values.yaml")

  if diff -u <(printf '%s\n' "$overlay") <(printf '%s\n' "$declared") >/tmp/rbac.diff 2>&1; then
    echo "✓ $dir: overlay matches image-declared RBAC"
    checked=$((checked + 1))
  else
    echo "✗ $dir: DRIFT — $dir/values.yaml differs from '$img tools rbac-values'"
    echo "    regenerate with:  docker run --rm $img tools rbac-values > $dir/values.yaml"
    sed 's/^/    /' /tmp/rbac.diff
    fail=1
  fi
done

echo "---"
echo "checked $checked module(s); $( [ $fail -eq 0 ] && echo OK || echo DRIFT )"
exit $fail
