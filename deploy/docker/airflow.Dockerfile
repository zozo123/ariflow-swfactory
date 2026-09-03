# syntax=docker/dockerfile:1
# swfactory orchestrator image for deploy/docker/compose.yml: Airflow 3 (`airflow standalone`)
# and the webhook receiver, run from the repo bind-mounted at its HOST path. Contains uv, git,
# gh (deliver: `gh pr create` when SWF_SCM=github) and the docker CLI (DockerSandbox spawns
# sibling containers through the mounted /var/run/docker.sock). `uv sync --group airflow`
# happens at container start (start.sh) into UV_PROJECT_ENVIRONMENT=/opt/venv (a named volume),
# never into the repo's own .venv.
FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends bash ca-certificates curl git gnupg procps \
 && mkdir -p -m 755 /etc/apt/keyrings \
 && curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg \
      -o /etc/apt/keyrings/githubcli-archive-keyring.gpg \
 && chmod go+r /etc/apt/keyrings/githubcli-archive-keyring.gpg \
 && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" \
      > /etc/apt/sources.list.d/github-cli.list \
 && apt-get update && apt-get install -y --no-install-recommends gh \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/
COPY --from=docker:cli /usr/local/bin/docker /usr/local/bin/docker

ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    PATH=/opt/venv/bin:/usr/local/bin:/usr/bin:/bin

CMD ["bash"]
