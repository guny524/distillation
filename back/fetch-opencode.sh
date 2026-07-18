#!/bin/sh
# Fetch the opencode CLI at container start -- used only by the comprehend stage.
#
# Why runtime, not baked: all seven workers share ONE image (they are the same
# `distill` binary run with a different subcommand). Only comprehend shells out
# to `opencode`; baking it would bloat the shared image for six workers that
# never use it AND lock the image to one arch (opencode is a Bun dynamic binary
# that cannot run under Rosetta amd64 emulation). Fetching at startup keeps the
# image small + arch-agnostic and pulls the release matching THIS container's
# arch. Idempotent: skips if already installed (so a restart with a persisted
# home does not re-download).
set -e

VERSION="${OPENCODE_VERSION:-1.18.3}"
DEST="${OPENCODE_BIN:-/home/distill/.local/bin/opencode}"

if [ -x "$DEST" ]; then
  echo "[fetch-opencode] already present: $DEST"
  exit 0
fi

case "$(uname -m)" in
  x86_64 | amd64) target=linux-x64 ;;
  aarch64 | arm64) target=linux-arm64 ;;
  *) echo "[fetch-opencode] unsupported arch: $(uname -m)" >&2; exit 1 ;;
esac

echo "[fetch-opencode] downloading opencode v${VERSION} (${target})"
tmp="$(mktemp -d)"
wget -qO "$tmp/opencode.tar.gz" \
  "https://github.com/anomalyco/opencode/releases/download/v${VERSION}/opencode-${target}.tar.gz"
tar -xzf "$tmp/opencode.tar.gz" -C "$tmp"
mkdir -p "$(dirname "$DEST")"
install -m 0755 "$(find "$tmp" -type f -name opencode | head -n1)" "$DEST"
rm -rf "$tmp"
echo "[fetch-opencode] installed: $DEST"
