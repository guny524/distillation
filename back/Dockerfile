FROM debian:bookworm-slim

# The shared worker image is just the static `distill` Go binary run with a
# different subcommand per stage (ingest/comprehend/question/presolve/answer/
# verify/flush). opencode is NOT baked here: only the comprehend stage uses it,
# and it fetches the arch-matching release at startup (see fetch-opencode.sh) so
# this image stays small and arch-agnostic. Tools kept: ca-certificates (HTTPS to
# subgate + GitHub), wget + tar (comprehend's runtime opencode fetch).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget tar \
    && rm -rf /var/lib/apt/lists/*

# non-root user with a writable home. opencode (comprehend only) writes its
# session/cache state and the fetched binary under $HOME/.local, so HOME must
# exist, be owned by the user, and be on PATH.
RUN groupadd -g 1001 distill \
    && useradd -u 1001 -g distill -m -d /home/distill -s /usr/sbin/nologin distill
ENV HOME=/home/distill
ENV PATH=/home/distill/.local/bin:/usr/local/bin:/usr/bin:/bin

WORKDIR /workspace
RUN mkdir -p /workspace/output && chown -R distill:distill /workspace

# distill: the one binary all workers run (locally cross-compiled, Makefile build
# target). fetch-opencode.sh: comprehend runs it at startup to pull opencode.
COPY distill /usr/local/bin/distill
COPY fetch-opencode.sh /usr/local/bin/fetch-opencode.sh
RUN chmod 0755 /usr/local/bin/fetch-opencode.sh

COPY --chown=distill:distill prompts/ /workspace/prompts/
COPY --chown=distill:distill config/ /workspace/config/
COPY --chown=distill:distill schemas/ /workspace/schemas/

# opencode's subgate provider (config/opencode.json) reads this key. subgate holds
# the real auth, so a placeholder satisfies the provider SDK; override via env
# (k8s Secret / compose .env) if the endpoint ever requires a real key.
ENV OPENCODE_SUBGATE_API_KEY=subgate-holds-auth

USER distill

# ENTRYPOINT only, no default CMD: every deployment (k8s workers/CronJobs/migrate Job,
# compose services/ofelia) passes an explicit subcommand (comprehend/question/.../migrate),
# so a bare `docker run <image>` prints `distill` help instead of silently running the
# DEPRECATED monolithic `run` cycle.
ENTRYPOINT ["distill"]
