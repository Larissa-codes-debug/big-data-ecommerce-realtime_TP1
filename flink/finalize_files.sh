#!/usr/bin/env bash
set -euo pipefail

STAGING="/var/lib/flink-input/staging"
READY="/var/lib/flink-input/ready"
STABLE_SECONDS="${STABLE_SECONDS:-15}"

mkdir -p "$STAGING" "$READY"

echo "[finalizer] Monitorando $STAGING -> $READY"
while true; do
  now="$(date +%s)"

  shopt -s nullglob
  for file in "$STAGING"/*.json; do
    [ -f "$file" ] || continue

    # Não mover arquivos modificados recentemente.
    mtime="$(stat -c %Y "$file" 2>/dev/null || echo "$now")"
    age=$((now - mtime))

    if [ "$age" -ge "$STABLE_SECONDS" ]; then
      name="$(basename "$file")"
      # mv é atômico dentro do mesmo volume/diretório de montagem.
      if mv -n -- "$file" "$READY/$name"; then
        echo "[finalizer] Arquivo pronto: $name"
      fi
    fi
  done

  sleep 5
done
