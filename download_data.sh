#!/usr/bin/env bash
# download_data.sh — Download NASA HTTP log files (July & August 1995)
#
# Official source: https://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
# Files:
#   ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz  (20.7 MB)
#   ftp://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz  (21.8 MB)
#
# Usage (from project root, inside WSL):
#   bash download_data.sh

set -euo pipefail

DATA_DIR="$(cd "$(dirname "$0")" && pwd)/data"
mkdir -p "$DATA_DIR"

# Primary: official ITA FTP server
FTP_BASE="ftp://ita.ee.lbl.gov/traces"

# Fallback mirrors (HTTP) — Kaggle & academic mirrors that re-host the dataset
HTTP_MIRRORS=(
  "http://ita.ee.lbl.gov/traces"
  "https://raw.githubusercontent.com/logpai/loghub/master/Apache"   # not these files but kept as placeholder
)

FILES=(
  "NASA_access_log_Jul95.gz"
  "NASA_access_log_Aug95.gz"
)

download_file() {
  local url="$1"
  local dest="$2"

  if command -v wget &>/dev/null; then
    wget --no-check-certificate --show-progress -O "$dest" "$url" && return 0
  fi
  if command -v curl &>/dev/null; then
    curl -L --insecure --progress-bar -o "$dest" "$url" && return 0
  fi
  echo "ERROR: neither wget nor curl found. Install one and retry." >&2
  return 1
}

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  NASA HTTP Log Downloader"
echo "  Target: $DATA_DIR"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

for fname in "${FILES[@]}"; do
  dest="$DATA_DIR/$fname"

  if [[ -f "$dest" ]]; then
    size=$(du -h "$dest" | cut -f1)
    echo "✓  Already present: $fname ($size)"
    continue
  fi

  echo ""
  echo "↓  Downloading $fname ..."

  # Try FTP first
  primary_url="$FTP_BASE/$fname"
  if download_file "$primary_url" "$dest"; then
    echo "✓  Saved: $dest"
  else
    echo "✗  FTP failed — trying HTTP fallback..."
    http_url="http://ita.ee.lbl.gov/traces/$fname"
    if download_file "$http_url" "$dest"; then
      echo "✓  Saved via HTTP: $dest"
    else
      echo ""
      echo "  ⚠  Automatic download failed for $fname."
      echo "  Please download it manually from:"
      echo "    $primary_url"
      echo "  and place it in: $DATA_DIR/"
      rm -f "$dest"   # remove partial download
    fi
  fi
done

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Files in $DATA_DIR:"
ls -lh "$DATA_DIR/"*.gz 2>/dev/null || echo "  (none found)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
