

# simpl-bulk-dataplane

FastAPI scaffold for a DPS-compatible data plane that targets S3 `PUSH` and `PULL` transfer types.

Preface: "I" built this in a couple of hours using agentic AI. It was a learning experience in using agentic AI to build a software project.

There are a few things that are not fully up to spec. WIP


## Project structure

```text
src/simpl_bulk_dataplane/
  api/               # HTTP routes and dependency wiring
  application/       # Use-case orchestration
  domain/            # State, entities, contracts, and signaling models
  infrastructure/    # In-memory repository and S3 transfer adapters
  main.py            # FastAPI app factory and local runner
src/simpl_bulk_manual_app/
  main.py            # Separate manual E2E UI app
  client.py          # Dataplane API client for the manual UI
  static/            # Served UI assets (React bundle + index)
  frontend/          # React + TypeScript source and build config
tests/               # Service and API smoke tests
```

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
uvicorn simpl_bulk_dataplane.main:app --reload --port 8080
```

Run the separate manual E2E UI app:

```bash
SIMPL_MANUAL_MQTT_ENABLED=true \
SIMPL_MANUAL_MQTT_HOST=localhost \
uvicorn simpl_bulk_manual_app.main:app --reload --port 8090
```

Rebuild manual UI frontend assets after UI changes:

```bash
cd src/simpl_bulk_manual_app/frontend
npm install
npm run build
```

Or via console scripts:

```bash
simpl-bulk-dataplane
simpl-bulk-manual-ui
```

Run full manual stack (manual UI + 2 dataplanes + 2 Postgres + RabbitMQ MQTT broker + 2 MinIO):

```bash
docker compose -f docker-compose.manual.yml up -d --build
```

Detailed manual playground walkthrough (UI examples, MinIO bucket/file setup, pause/resume):
- `README.manual.md`

If you changed startup settings, recreate services:

```bash
docker compose -f docker-compose.manual.yml down -v
docker compose -f docker-compose.manual.yml up -d --build
```

Endpoints:
- Manual UI: `http://localhost:18090`
- Dataplane A: `http://localhost:18081`
- Dataplane B: `http://localhost:18082`
- RabbitMQ MQTT: `localhost:1883`
- RabbitMQ management: `http://localhost:15672` (`guest` / `guest`)
- MinIO A API/console: `http://localhost:19000` / `http://localhost:19001`
- MinIO B API/console: `http://localhost:19010` / `http://localhost:19011`

## Run tests

```bash
pytest -q
```

Run Docker end-to-end test (2 MinIO + 2 dataplanes, 100MB transfer, suspend/resume):

```bash
RUN_DOCKER_E2E=1 pytest -q -m e2e tests/e2e/test_transfer_resume_e2e.py
```

Run local Postgres (for `postgres` repository backend):

```bash
docker compose up -d postgres
```

Then run the dataplane with Postgres persistence:

```bash
SIMPL_DP_REPOSITORY_BACKEND=postgres \
SIMPL_DP_POSTGRES_DSN=postgresql://simpl:simpl@localhost:5432/simpl_dataplane \
uvicorn simpl_bulk_dataplane.main:app --reload --port 8080
```

## Management endpoints (separate route)

- `GET /management/dataflows`
  - Optional query: `mode=PUSH|PULL`
  - Returns all flows with state + progress (`bytesTotal`, `bytesTransferred`, `percentComplete`, `running`, `paused`, `finished`, `lastError`)
- `GET /management/dataflows/{id}`
  - Returns one flow with the same progress payload

Pause/resume behavior remains on signaling endpoints:
- Pause: `POST /dataflows/{id}/suspend`
- Resume:
  - Push flows: call `POST /dataflows/start` again
  - Pull flows: call `POST /dataflows/{id}/started` (or replay `start` + `started`)

## MQTT dataflow events (RabbitMQ MQTT plugin compatible)

Dataplane can publish dataflow state/progress events to MQTT:

```bash
SIMPL_DP_DATAFLOW_EVENTS_MQTT_ENABLED=true
SIMPL_DP_DATAFLOW_EVENTS_MQTT_HOST=localhost
SIMPL_DP_DATAFLOW_EVENTS_MQTT_PORT=1883
SIMPL_DP_DATAFLOW_EVENTS_MQTT_TOPIC_PREFIX=simpl/dataplane
SIMPL_DP_DATAFLOW_EVENTS_MQTT_QOS=0
# optional
SIMPL_DP_DATAFLOW_EVENTS_MQTT_USERNAME=guest
SIMPL_DP_DATAFLOW_EVENTS_MQTT_PASSWORD=guest
SIMPL_DP_DATAPLANE_PUBLIC_URL=http://localhost:8080
```

Published topics:
- `<topicPrefix>/<dataplaneId>/dataflows/<dataFlowId>/state`
- `<topicPrefix>/<dataplaneId>/dataflows/<dataFlowId>/progress`

Manual UI consumes MQTT events directly (no dataplane polling fallback):

```bash
SIMPL_MANUAL_MQTT_ENABLED=true
SIMPL_MANUAL_MQTT_HOST=localhost
SIMPL_MANUAL_MQTT_PORT=1883
SIMPL_MANUAL_MQTT_TOPIC_PREFIX=simpl/dataplane
# optional
SIMPL_MANUAL_MQTT_USERNAME=guest
SIMPL_MANUAL_MQTT_PASSWORD=guest
```

Manual UI live stream endpoint:
- `GET /ws/dataflows?dataplaneUrls=http://dp-a:8080,http://dp-b:8080` (WebSocket)

## Control-plane registration and callbacks

Dataplane can register itself on startup and then send state callbacks to control-plane endpoints from `docs/signaling-openapi.yaml`:
- `POST /transfers/{transferId}/dataflow/prepared`
- `POST /transfers/{transferId}/dataflow/started`
- `POST /transfers/{transferId}/dataflow/completed`
- `POST /transfers/{transferId}/dataflow/errored`

Enable startup registration:

```bash
SIMPL_DP_CONTROL_PLANE_REGISTRATION_ENABLED=true
SIMPL_DP_CONTROL_PLANE_ENDPOINT=https://controlplane.example.com/signaling/v1
SIMPL_DP_DATAPLANE_PUBLIC_URL=https://dataplane.example.com/signaling/v1
# optional
SIMPL_DP_CONTROL_PLANE_TIMEOUT_SECONDS=10
SIMPL_DP_CONTROL_PLANE_REGISTRATION_NAME=My Dataplane
SIMPL_DP_CONTROL_PLANE_REGISTRATION_DESCRIPTION=Dataplane for team X
SIMPL_DP_CONTROL_PLANE_REGISTRATION_TRANSFER_TYPES=com.test.s3-PUSH,com.test.s3-PULL
SIMPL_DP_CONTROL_PLANE_REGISTRATION_LABELS=team-x,prod
SIMPL_DP_CONTROL_PLANE_REGISTRATION_AUTHORIZATION=[{"type":"oauth2_client_credentials"}]
```

Behavior:
- On startup, dataplane tries `POST /dataplanes/register` on the configured control plane.
- If registration succeeds, HTTP callbacks are enabled.
- If registration fails (or config is incomplete), dataplane falls back to noop callbacks.

## Notes

- Endpoints are scaffolded from `docs/signaling-openapi.yaml`.
- Models mirror `docs/docs/schemas/*.json` with Pydantic aliases for DPS JSON field names.
- Transfer execution is async and in-memory session based:
  - `start`/`notify_started` trigger background S3 copy tasks.
  - active transfer sessions are capped by `SIMPL_DP_S3_MAX_ACTIVE_DATAFLOWS` (default `4`); overflow flows wait in an internal queue.
  - boto3 connection pooling is configurable via `SIMPL_DP_S3_MAX_POOL_CONNECTIONS` (default `16`).
  - effective multipart concurrency is capped by the configured S3 pool size.
  - slot queueing is implemented as reusable runtime utility (`SlotBasedExecutionQueue`) for future non-S3 executors.
  - `suspend` pauses execution and a subsequent `start` resumes.
  - multipart copy is used for large objects or `forceMultipart=true`.
  - when source and destination use the same S3 endpoint, transfers prefer server-side copy APIs (`copy_object` / `upload_part_copy`) to avoid payload relay through dataplane.
  - server-side copy can be disabled via `SIMPL_DP_S3_PREFER_SERVER_SIDE_COPY=false`.
  - runtime intentionally stays on `boto3` + bounded thread offload; evaluate async SDK migration only after profiling a real bottleneck.
  - Omitting `sourceKey` copies all objects in `sourceBucket` (bucket-to-bucket mode).
  - In bucket mode, `destinationKey` is optional and acts as a destination prefix when set.
- Useful metadata keys for S3 execution:
  - `sourceBucket`, `sourceKey`, `destinationBucket`, `destinationKey`
  - `sourceDataAddress`, `destinationDataAddress` (DataAddress-shaped objects)
  - `<target>EndpointUrl`, `<target>ForcePathStyle` (`target` = `source` or `destination`)
  - `<target>AccessKeyId`, `<target>SecretAccessKey`, `<target>SessionToken`
  - `multipartThresholdMb`, `multipartPartSizeMb`, `multipartConcurrency`
- Repository backend configuration:
  - `SIMPL_DP_REPOSITORY_BACKEND=in_memory` (default) or `postgres`
  - `SIMPL_DP_POSTGRES_DSN` is required when backend is `postgres`
