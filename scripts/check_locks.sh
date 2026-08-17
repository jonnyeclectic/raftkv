#!/usr/bin/env bash
# The two lockfiles answer to different consumers, and nothing else notices when they
# disagree.
#
#   uv.lock            what `uv sync` resolves, and therefore what the whole suite runs against
#   requirements.lock  what the Dockerfile installs, hash-checked, and therefore what the
#                      compose cluster, the kind StatefulSet and any published image run
#
# Change a dependency and re-sync without re-exporting, and the suite proves one
# resolution green while every container ships the other. No test fails. The smoke test
# passes. The difference surfaces as a behavioural mystery in a container, which is the
# most expensive possible place to find it.
#
# Both directions are checked here: that uv.lock still matches pyproject.toml, and that
# requirements.lock is exactly what re-exporting uv.lock produces. Regenerating and
# diffing is the whole idea — there is no cleverness to get wrong.
#
# Run by `make locks`, by `make gate`, and by the `provenance` job in CI, so local and
# remote are the same check rather than two implementations of the same intent.
set -euo pipefail
cd "$(dirname "$0")/.."

work=".lockcheck"
trap 'rm -rf "$work"' EXIT
rm -rf "$work"
mkdir -p "$work"

echo "== uv.lock agrees with pyproject.toml =="
uv lock --check

echo "== requirements.lock agrees with uv.lock =="
uv export --frozen --no-dev --no-emit-project -o "$work/requirements.lock" >/dev/null 2>&1

# uv stamps the command it was given — including the output path — into line 2, so the
# comparison starts at line 3. Everything from there down is the resolution itself.
tail -n +3 requirements.lock > "$work/have"
tail -n +3 "$work/requirements.lock" > "$work/want"

if diff -u "$work/have" "$work/want" > "$work/delta"; then
  echo "LOCKS OK: uv.lock and requirements.lock describe the same dependency set"
else
  echo >&2
  echo "requirements.lock is stale. Run 'make relock' and commit BOTH lockfiles." >&2
  echo "First 30 lines of the difference (- committed, + what uv.lock resolves to):" >&2
  head -30 "$work/delta" >&2
  exit 1
fi
