# Docker Deployment

This setup only manages dexmoney app processes. It does not create or recreate
the existing Postgres, Redis, or Base node containers.

## Files

- `docker-compose.apps.yml`: market-data, searcher, monitor-web, execution-manager.
- `.env`: normal runtime config and secrets.
- `.env.docker`: Docker-only network/URL overrides copied from `.env.docker.example`.
- `docker-compose.portainer.yml`: optional Portainer UI for container management.

## Initial Setup

```bash
cd ~/dexmoney
cp .env.docker.example .env.docker
```

For the current server, the defaults are expected to work:

```bash
DEXMONEY_DOCKER_NETWORK=dexmoney_default
BASE_NODE_DOCKER_NETWORK=node_default
POSTGRES_URL_DOCKER=postgres://user:password@base-arb-postgres:5432/base_arb
REDIS_URL_DOCKER=redis://base-arb-redis:6379
BASE_RPC_HTTP_DOCKER=http://node-execution-1:8545
BASE_RPC_WS_DOCKER=ws://node-execution-1:8546
```

## Start Non-Executor Services

```bash
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml up -d --build market-data searcher monitor-web
```

## Start Or Restart Executor

The executor is behind the `executor` profile so a normal `up -d` does not start
trading by accident.

```bash
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml --profile executor up -d --build execution-manager
```

## Common Operations

```bash
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml ps
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml logs -f market-data
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml logs -f searcher
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml logs -f monitor-web
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml --profile executor logs -f execution-manager
```

```bash
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml restart market-data searcher monitor-web
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml --profile executor restart execution-manager
```

```bash
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml stop execution-manager
sudo docker compose --env-file .env.docker -f docker-compose.apps.yml down
```

## Portainer UI

Portainer can manage Docker containers from a web UI. It mounts the Docker
socket, so it is effectively root-equivalent. Keep it bound to localhost and use
an SSH tunnel, or put it behind a secured reverse proxy.

```bash
sudo docker compose -f docker-compose.portainer.yml up -d
```

Open it through an SSH tunnel:

```bash
ssh -L 9443:127.0.0.1:9443 ubuntu@YOUR_SERVER
```

Then visit `https://127.0.0.1:9443`.

## Securing Existing Database Ports

If host access is still needed, bind DB/Redis to localhost in the DB compose:

```yaml
ports:
  - "127.0.0.1:5632:5432"
```

```yaml
ports:
  - "127.0.0.1:6779:6379"
```

If only containers need DB/Redis access, remove the `ports` entries entirely.
