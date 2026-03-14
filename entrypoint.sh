#!/bin/bash
set -euo pipefail

echo "[entrypoint] Starting codex exec..."

codex exec \
  --full-auto \
  --model gpt-5.4 \
  --sandbox workspace-write \
  -C /workspace \
  < /workspace/prompts/distillation.md

echo "[entrypoint] codex exec finished."

# Post-process: load result to DB
if [ -f /workspace/output/result.jsonl ]; then
  echo "[entrypoint] Loading result.jsonl to PostgreSQL..."
  python3 /workspace/scripts/load_to_db.py /workspace/output/result.jsonl
  echo "[entrypoint] Load complete."
else
  echo "[entrypoint] No result.jsonl found, skipping DB load."
fi
