#!/usr/bin/env bash
# Export an Apple Photos album to a folder Fashion Intel can import (macOS only).
#   ./scripts/export-apple-photos.sh "Album Name" ~/Pictures/fashion-intel-export
# Requires osxphotos:  pipx install osxphotos   (or: pip3 install osxphotos)
set -euo pipefail
ALBUM="${1:?Usage: export-apple-photos.sh \"Album Name\" [dest-folder]}"
DEST="${2:-$HOME/Pictures/fashion-intel-export}"
if ! command -v osxphotos >/dev/null 2>&1; then
  echo "osxphotos is not installed. Install it with: pipx install osxphotos" >&2
  exit 1
fi
mkdir -p "$DEST"
osxphotos export "$DEST" --album "$ALBUM" --convert-to-jpeg --jpeg-quality 0.9 --skip-original-if-edited
echo
echo "Done. In Fashion Intel, open the import tray → From folder → paste: $DEST"
