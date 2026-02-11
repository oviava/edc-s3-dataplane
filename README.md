# simpl-bulk-dataplane

FastAPI scaffold for a DPS-compatible data plane that targets S3 `PUSH` and `PULL` transfer types.

## Project structure

```text
src/simpl_bulk_dataplane/
  api/               # HTTP routes and dependency wiring
  application/       # Use-case orchestration
  domain/            # State, entities, contracts, and signaling models
  infrastructure/    # In-memory repository and S3 transfer adapters
  main.py            # FastAPI app factory and local runner
tests/               # Service and API smoke tests
```

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
uvicorn simpl_bulk_dataplane.main:app --reload --port 8080
```

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

## Notes

- Endpoints are scaffolded from `docs/signaling-openapi.yaml`.
- Models mirror `docs/docs/schemas/*.json` with Pydantic aliases for DPS JSON field names.
- Transfer execution is async and in-memory session based:
  - `start`/`notify_started` trigger background S3 copy tasks.
  - `suspend` pauses execution and a subsequent `start` resumes.
  - multipart copy is used for large objects or `forceMultipart=true`.
- Useful metadata keys for S3 execution:
  - `sourceBucket`, `sourceKey`, `destinationBucket`, `destinationKey`
  - `sourceDataAddress`, `destinationDataAddress` (DataAddress-shaped objects)
  - `<target>EndpointUrl`, `<target>ForcePathStyle` (`target` = `source` or `destination`)
  - `<target>AccessKeyId`, `<target>SecretAccessKey`, `<target>SessionToken`
  - `multipartThresholdMb`, `multipartPartSizeMb`, `multipartConcurrency`
- Repository backend configuration:
  - `SIMPL_DP_REPOSITORY_BACKEND=in_memory` (default) or `postgres`
  - `SIMPL_DP_POSTGRES_DSN` is required when backend is `postgres`
