#!/usr/bin/env bash
set -euo pipefail

RESULTS=criterion-summary.json
: > "$RESULTS"

shopt -s globstar nullglob

declare -A picked
files=(target/criterion/**/new/estimates.json target/criterion/**/estimates.json)
for f in "${files[@]}"; do
	[[ -f "$f" ]] || continue
	benchdir=$(sed -E 's#^target/criterion/([^/]+)/.*#\1#' <<<"$f")
	# Prefer 'new' over root estimates if both exist
	if [[ -n "${picked[$benchdir]:-}" && "$f" != */new/* ]]; then
		continue
	fi
	mean_sec=$(jq -r '.Mean.point_estimate' "$f")
	# Convert seconds to milliseconds for readability
	ms=$(awk -v s="$mean_sec" 'BEGIN{printf "%.3f", s*1000}')
	if [[ ! -s "$RESULTS" ]]; then echo '[]' > "$RESULTS"; fi
	tmp=$(mktemp)
	jq --arg name "$benchdir" --argjson value "$ms" '. + [{"name":$name,"value":$value}]' "$RESULTS" > "$tmp"
	mv "$tmp" "$RESULTS"
	picked[$benchdir]=1
done

echo 'Generated summary:'
cat "$RESULTS"