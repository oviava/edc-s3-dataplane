# Bulk Data Transfer: Technical Description

## 1. Purpose and Scope

This document describes the current `simpl-bulk-dataplane` implementation from an architecture perspective and evaluates whether it fulfills the functional requirements for Bulk Data Transfer.

Scope covered here:

- dataplane signaling API and state machine
- S3 transfer execution runtime
- persistence, monitoring, and control-plane callbacks
- requirement coverage and identified gaps

## 2. Architecture Overview

The implementation follows a layered/ports-and-adapters style:

- Domain layer: transfer state model, message models, ports
- Application layer: `DataFlowService` orchestrates lifecycle and validation
- Infrastructure layer: S3 execution adapter, repositories, control-plane notifier, MQTT events
- API layer: FastAPI signaling and management endpoints

Key components:

- `DataFlowService` (`/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/application/services/dataflow_service.py`)
- `S3TransferExecutor` (`/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/infrastructure/transfers/s3_transfer_executor.py`)
- signaling models (`/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/domain/signaling_models.py`)
- signaling routes (`/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/api/routes/signaling.py`)
- management routes (`/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/api/routes/management.py`)
- repository adapters (`/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/infrastructure/repositories`)

## 3. Functional Building Blocks

### 3.1 Transfer lifecycle and orchestration

`DataFlowService` implements prepare/start/started/suspend/resume/terminate/completed transitions and persists state per `dataFlowId`. It enforces transfer-mode-specific rules for `dataAddress` handling and transition validity.

### 3.2 Resource address handling (S3)

S3 addresses are handled via `DataAddress` and metadata keys:

- `sourceBucket`, `sourceKey`, `destinationBucket`, `destinationKey`
- `sourceDataAddress`, `destinationDataAddress`
- endpoint/credential overrides via `endpointProperties`

The executor can parse both `s3://bucket[/key]` and `http(s)://host/bucket[/key]` forms.

### 3.3 Asynchronous chunked data transfer

`S3TransferExecutor` runs copies in background asyncio tasks, with:

- multipart transfer for large objects
- configurable threshold/part size/concurrency
- suspend/resume via pause events
- terminate with multipart abort support
- progress snapshots (`bytesTotal`, `bytesTransferred`, percent, running/paused/finished)

It supports object and bucket-to-bucket copy modes.

### 3.4 Control-plane integration and observability

- Optional control-plane registration and state callbacks (`prepared`, `started`, `completed`, `errored`)
- Optional MQTT state/progress events
- Management APIs for querying flow and progress state

## 4. End-to-End Runtime View

### 4.1 PUSH flow (provider sends to consumer destination)

1. Control plane calls `/dataflows/prepare` (optional async) to obtain destination hints.
2. Control plane calls `/dataflows/start` with destination `dataAddress`.
3. Dataplane creates/resumes a background S3 copy session.
4. Runtime completion promotes flow to `COMPLETED` and emits callbacks/events.

### 4.2 PULL flow (consumer pulls from provider source)

1. Provider side `/dataflows/start` returns source `dataAddress`.
2. Consumer side `/dataflows/{id}/started` provides source address (or persisted one on resume).
3. Dataplane performs asynchronous S3 copy to consumer-side destination.

## 5. Requirement Coverage Assessment

### Functional requirement 1
"Register a bulk data transfer mechanism as a resource sharing method"

Status: **Partial**

What is implemented:

- control-plane registration includes supported transfer types (`transferTypes`)
- S3-specific address construction/parsing exists in runtime

Gaps:

- no explicit mechanism registry or pluggable transfer-mechanism catalog in domain/application
- implementation is hard-limited to S3 transfer types (`ensure_s3_transfer_type`)

### Functional requirement 2
"Define destination for bulk data transfer"

Status: **Met for S3**

What is implemented:

- destination can be provided as `dataAddress` on start for PUSH
- destination/source can also be modeled through metadata and `DataAddress`
- destination bucket/prefix semantics are implemented for object and bucket modes

### Functional requirement 3
"Execute bulk data transfer asynchronously"

Status: **Met for S3**

What is implemented:

- async task-based execution
- chunked multipart transfer and configurable parallelism
- suspend/resume/terminate lifecycle
- runtime progress and completion signaling

## 6. Does the Current Implementation Fulfill the Requirements?

Short answer: **partially**.

- For **S3 Bulk Data Transfer execution**, the implementation fulfills the core requirements (asynchronous chunked transfer, destination definition, lifecycle handling).
- For the broader **"over-arching core capability" across multiple technologies**, it is not fully complete yet because the architecture currently enforces S3-only transfer types and lacks a first-class extensibility mechanism for additional transfer technologies.

## 7. Evidence from Tests

Current test suite result: `51 passed, 1 skipped`.

Representative coverage:

- multipart async transfer behavior
- suspend/resume continuation
- bucket-to-bucket copy with destination prefix mapping
- API validation of start/prepare/resume semantics
- management progress/state responses
- optional control-plane registration/callback wiring

Files:

- `/Users/ovidiua/projects/simpl-bulk-dataplane/tests/test_s3_transfer_executor.py`
- `/Users/ovidiua/projects/simpl-bulk-dataplane/tests/test_dataflow_service.py`
- `/Users/ovidiua/projects/simpl-bulk-dataplane/tests/test_api.py`
- `/Users/ovidiua/projects/simpl-bulk-dataplane/tests/test_management_api.py`
- `/Users/ovidiua/projects/simpl-bulk-dataplane/tests/e2e/test_transfer_resume_e2e.py`

## 8. Architecture Gaps and Recommended Next Steps

1. Introduce a transfer-mechanism plugin model
- Add a mechanism registry port and per-mechanism executor/address-template adapters.

2. Replace S3 hard-gating with capability routing
- Move transfer-type validation from global S3 check to registry lookup.

3. Formalize technology-specific resource address templates
- Define explicit template schemas/validators (required/optional fields) per mechanism.

4. Clarify contract/policy enforcement boundary
- Either integrate explicit policy checks in dataplane or document that control plane is the sole enforcement point.

5. Improve resumability durability
- Persist runtime checkpoints (multipart upload state) if restart-safe resumability is required.
