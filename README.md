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

## Notes

- Endpoints are scaffolded from `docs/signaling-openapi.yaml`.
- Models mirror `docs/docs/schemas/*.json` with Pydantic aliases for DPS JSON field names.
- Repository and transfer execution are intentionally pluggable; the current implementation is in-memory plus S3-oriented placeholders.
