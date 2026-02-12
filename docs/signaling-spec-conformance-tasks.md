# Signaling Spec Conformance Tasks

Source of truth used for checks: `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md`.

## High Priority

- [ ] Enforce `/dataflows/start` conflict on existing `processId` (return 4xx).
  - Spec: `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md:346`
  - Current implementation:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/application/services/dataflow_service.py:112`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/application/services/dataflow_service.py:122`

- [ ] Enforce terminal state immutability (`COMPLETED` and `TERMINATED` must not transition further).
  - Spec:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md:90`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md:93`
  - Current implementation:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/application/services/dataflow_service.py:225`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/application/services/dataflow_service.py:240`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/application/services/dataflow_service.py:244`

- [ ] Restrict `completed` transition to valid state-machine path (`STARTED -> COMPLETED`).
  - Spec:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md:73`
  - Current implementation:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/application/services/dataflow_service.py:235`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/application/services/dataflow_service.py:240`

- [ ] Implement provider-push completion signaling sequence so completion is emitted without manual `/completed`.
  - Spec sequence:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md:174`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md:176`
  - Current implementation/evidence:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/infrastructure/transfers/s3_transfer_executor.py:338`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/application/services/dataflow_service.py:235`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/tests/e2e/test_transfer_resume_e2e.py:288`

- [ ] Align callback routing with request-level `callbackAddress` semantics.
  - Spec:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md:263`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md:340`
  - Current implementation:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/application/services/dataflow_service.py:297`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/bootstrap.py:88`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/bootstrap.py:90`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/bootstrap.py:137`

## Medium Priority

- [ ] Make `/dataflows/:id/completed` method match signaling spec (`POST` as canonical documented method).
  - Spec:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md:481`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md:482`
  - Current implementation:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/api/routes/signaling.py:157`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/api/routes/signaling.py:158`

- [ ] Make `callbackAddress` required in prepare/start models (or explicitly reconcile docs/spec mismatch).
  - Spec:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md:263`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md:340`
  - Current implementation:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/domain/signaling_models.py:57`

## Low Priority

- [x] Adopt `INITIALIZED` as the canonical initial state in docs and implementation.
  - Spec:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/docs/docs/signaling.md:83`
  - Current implementation:
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/domain/transfer_types.py:11`
    - `/Users/ovidiua/projects/simpl-bulk-dataplane/src/simpl_bulk_dataplane/domain/entities.py:26`
