#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${ROOT_DIR}/build"

mkdir -p "${OUT_DIR}"
cd "${ROOT_DIR}"

echo "[build] output: ${OUT_DIR}/yt-bridge"
CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o "${OUT_DIR}/yt-bridge" .

echo "[build] done"
