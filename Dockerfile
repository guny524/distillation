FROM node:22-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir --break-system-packages \
    psycopg2-binary

RUN npm install -g @openai/codex

# non-root user
RUN groupadd --system --gid 1001 distill \
    && useradd --system --uid 1001 --gid distill codexuser

WORKDIR /workspace

RUN mkdir -p /workspace/output && chown -R codexuser:distill /workspace

COPY --chown=codexuser:distill prompts/ /workspace/prompts/
COPY --chown=codexuser:distill config/ /workspace/config/
COPY --chown=codexuser:distill schemas/ /workspace/schemas/
COPY --chown=codexuser:distill scripts/ /workspace/scripts/
COPY --chown=codexuser:distill entrypoint.sh /workspace/entrypoint.sh

RUN chmod +x /workspace/entrypoint.sh

USER codexuser

ENTRYPOINT ["bash", "/workspace/entrypoint.sh"]
