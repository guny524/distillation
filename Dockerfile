FROM alpine:3.21

ARG CODEX_VERSION=0.115.0

# ca-certificates for HTTPS (codex -> OpenAI API)
RUN apk add --no-cache ca-certificates

# codex CLI (Rust binary from GitHub releases, musl static build)
RUN wget -qO /tmp/codex.tar.gz \
    "https://github.com/openai/codex/releases/download/rust-v${CODEX_VERSION}/codex-x86_64-unknown-linux-musl.tar.gz" \
    && tar xzf /tmp/codex.tar.gz -C /usr/local/bin/ \
    && rm /tmp/codex.tar.gz \
    && chmod +x /usr/local/bin/codex

# non-root user with home directory for codex auth
RUN addgroup -g 1001 -S distill \
    && adduser -u 1001 -S -G distill -h /home/codexuser codexuser

ENV CODEX_HOME=/home/codexuser/.codex
RUN mkdir -p /home/codexuser/.codex && chown -R codexuser:distill /home/codexuser

WORKDIR /workspace

RUN mkdir -p /workspace/output && chown -R codexuser:distill /workspace

# Go binary (locally cross-compiled, see Makefile build target)
COPY distill /usr/local/bin/distill

COPY --chown=codexuser:distill prompts/ /workspace/prompts/
COPY --chown=codexuser:distill config/ /workspace/config/
COPY --chown=codexuser:distill schemas/ /workspace/schemas/
COPY --chown=codexuser:distill entrypoint.sh /workspace/entrypoint.sh

RUN chmod +x /workspace/entrypoint.sh

USER codexuser

ENTRYPOINT ["sh", "/workspace/entrypoint.sh"]
