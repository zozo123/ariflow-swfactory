# deploy/docker — fully local factory (for TESTING)

Compose files and images only; the stack, the knobs and the honest limits (the Docker socket is
root-equivalent, a container is not a MicroVM, no phantom tokens) are documented in
[../../docs/docker.md](../../docs/docker.md).

Always run from the **repo root**: compose mounts `${PWD}` at `${PWD}` so a run's workdir has the
same absolute path on the host, in the Airflow container and in every sandbox container.

```bash
docker build -t swfactory-sandbox:local -f deploy/docker/sandbox.Dockerfile .
#   Linux: --build-arg UID=$(id -u) --build-arg GID=$(id -g) so the agent's files are yours
docker compose -f deploy/docker/compose.yml up          # airflow :8080 + webhook receiver :8081
docker compose -f deploy/docker/compose.yml exec airflow \
  cat /opt/airflow_home/simple_auth_manager_passwords.json.generated    # the admin password
docker compose -f deploy/docker/compose.yml down
# Add --volumes to also drop the DB, venv and generated password.

# one-shot, no Airflow: the CLI on the host, one sandbox container per command
uv run swfactory demo --sandbox docker
```
