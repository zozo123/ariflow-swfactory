# syntax=docker/dockerfile:1
# swfactory agent sandbox image: what every `DockerSandbox.run()` executes in.
#   docker build -t swfactory-sandbox:local -f deploy/docker/sandbox.Dockerfile .
# Contents: python 3.12, git, curl, node 22 (nodesource), Claude Code, uv, non-root user `swf`
# ($HOME=/home/swf, the path DockerSandbox mounts ~/.claude into for credentials=host).
# Build with --build-arg UID=$(id -u) --build-arg GID=$(id -g) on Linux so files the agent
# writes into the bind-mounted workdir are owned by you, not by uid 1000.
FROM python:3.12-slim

ARG UID=1000
ARG GID=1000
ARG NODE_MAJOR=22
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y --no-install-recommends bash ca-certificates curl git gnupg procps \
 && curl -fsSL "https://deb.nodesource.com/setup_${NODE_MAJOR}.x" | bash - \
 && apt-get install -y --no-install-recommends nodejs \
 && npm install -g @anthropic-ai/claude-code \
 && npm cache clean --force \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN groupadd -g "${GID}" swf && useradd -m -u "${UID}" -g "${GID}" -s /bin/bash swf
# uv system-wide so any uid (DockerSandbox runs as the host uid on Linux) finds it on PATH.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
USER swf
ENV HOME=/home/swf \
    PATH=/home/swf/.local/bin:/usr/local/bin:/usr/bin:/bin \
    UV_LINK_MODE=copy \
    CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1
WORKDIR /home/swf
# ~/.cache is a named volume at run time; creating it here (as swf) gives the volume swf ownership.
RUN mkdir -p /home/swf/.cache \
 && uv --version && claude --version && git --version

CMD ["bash"]
