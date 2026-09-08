# deploy/docker — fully local factory (for TESTING)

Compose files and images only; the stack, the knobs and the honest limits (the Docker socket is
root-equivalent, a container is not a MicroVM, no phantom tokens) are documented in
[../../docs/docker.md](../../docs/docker.md).

Always run from the **repo root**: compose mounts `${PWD}` at `${PWD}` so a run's workdir has the
same absolute path on the host, in the Airflow container and in every sandbox container.

```bash
docker build -t swfactory-sandbox:local -f deploy/docker/sandbox.Dockerfile .
#   Linux: --build-arg UID=$(id -u) --build-arg GID=$(id -g) so the agent's files are yours
# 32+ non-whitespace characters: the console authenticates with it AND managed work cells send it
# from inside the Airflow worker (a stack without it fails every managed job in its first stage).
export SWF_BACKEND_TOKEN=...
docker compose -f deploy/docker/compose.yml up   # airflow :8080, webhook :8081, backend :8082
docker compose -f deploy/docker/compose.yml exec airflow \
  cat /opt/airflow_home/simple_auth_manager_passwords.json.generated    # the admin password
docker compose -f deploy/docker/compose.yml down
# Add --volumes to also drop the DB, venv and generated password.

# one-shot, no Airflow: the CLI on the host, one sandbox container per command.
# SWF_DOCKER_IMAGE is required: the built-in default is an unpublished ghcr.io image, so without
# it the run fails with `registry: denied` rather than using the image you just built.
SWF_DOCKER_IMAGE=swfactory-sandbox:local uv run swfactory demo --sandbox docker
```

To run the factory against **itself** on this stack, see [../../docs/selfhost.md](../../docs/selfhost.md).
