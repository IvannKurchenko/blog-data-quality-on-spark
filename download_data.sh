#!/bin/sh
set -eu

BASE="https://d37ci6vzurychx.cloudfront.net/trip-data"
TYPES="yellow green"
YEARS="2024 2025"

for year in $YEARS; do
  for month in $(seq 1 12); do
    ym=$(printf "%04d-%02d" "$year" "$month")
    for t in $TYPES; do
      folder="./data/${t}"
      mkdir -p "$folder"

      file="${t}_tripdata_${ym}.parquet"
      url="${BASE}/${file}"
      out="${folder}/${file}"

      if [ -f "$out" ]; then
        echo "Already have $out, skipping."
        continue
      fi

      echo "Downloading $url -> $out"
      if ! curl -fL --retry 3 --retry-delay 2 -o "$out" "$url"; then
        echo "Missing or failed: $url (skipping)"
        rm -f "$out" 2>/dev/null || true
      fi
    done
  done
done