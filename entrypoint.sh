#!/bin/sh
set -eu

echo "[entrypoint] Starting codex exec..."

codex exec \
  --full-auto \
  --model gpt-5.4 \
  -C /workspace \
  < /workspace/prompts/distillation.md

echo "[entrypoint] codex exec finished."

# Post-process: load result to DB
if [ -f /workspace/output/result.jsonl ]; then
  echo "[entrypoint] Loading result.jsonl to PostgreSQL..."
  distill load /workspace/output/result.jsonl
  echo "[entrypoint] Load complete."
else
  echo "[entrypoint] No result.jsonl found, skipping DB load."
fi
