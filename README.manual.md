# Manual Playground Guide (`docker-compose.manual.yml`)

This guide shows how to run the manual stack and use the UI to test transfers between two dataplanes with two MinIO instances.

## 1) Start the manual stack

```bash
docker compose -f docker-compose.manual.yml up -d --build
```

If you want a clean reset:

```bash
docker compose -f docker-compose.manual.yml down -v
docker compose -f docker-compose.manual.yml up -d --build
```

## 2) Endpoints and credentials

- Manual UI: `http://localhost:18090`
- Dataplane A: `http://localhost:18081`
- Dataplane B: `http://localhost:18082`
- MinIO A API/Console: `http://localhost:19000` / `http://localhost:19001`
- MinIO B API/Console: `http://localhost:19010` / `http://localhost:19011`
- RabbitMQ management: `http://localhost:15672` (`guest` / `guest`)
- MinIO credentials (both instances): `minioadmin` / `minioadmin`

Important for UI form values:
- Use `http://dataplane-a:8080` or `http://dataplane-b:8080` as `Dataplane URL`.
- Use MinIO container endpoints in the form: `http://minio-a:9000` and `http://minio-b:9000`.
- `sourceAccessKeyId` and `sourceSecretAccessKey` must be set together (same for destination credentials).

## 3) Create buckets and test files in MinIO

### Option A: MinIO console (easiest)

1. Open `http://localhost:19001` (MinIO A console), log in with `minioadmin` / `minioadmin`.
2. Create bucket `source-bucket`.
3. Upload one or more test files (for example `payload-100mb.bin`) into `source-bucket`.
4. Open `http://localhost:19011` (MinIO B console), log in with `minioadmin` / `minioadmin`.
5. Create bucket `destination-bucket`.

### Option B: CLI with `mc` (if installed)

```bash
mc alias set minio-a http://localhost:19000 minioadmin minioadmin
mc alias set minio-b http://localhost:19010 minioadmin minioadmin

mc mb --ignore-existing minio-a/source-bucket
mc mb --ignore-existing minio-a/destination-bucket
mc mb --ignore-existing minio-b/source-bucket
mc mb --ignore-existing minio-b/destination-bucket

head -c 104857600 /dev/urandom > /tmp/payload-100mb.bin
mc cp /tmp/payload-100mb.bin minio-a/source-bucket/payload-100mb.bin
```

## 4) Example UI inputs

Open `http://localhost:18090` and use these form values.

### Example A: PUSH one file (MinIO A -> MinIO B via dataplane-a)

- Dataplane URL: `http://dataplane-a:8080`
- Transfer mode: `PUSH`
- Source bucket: `source-bucket`
- Source key: `payload-100mb.bin`
- Destination bucket: `destination-bucket`
- Destination key: `payload-100mb-copy.bin`
- Source endpoint URL: `http://minio-a:9000`
- Destination endpoint URL: `http://minio-b:9000`
- Source access key ID: `minioadmin`
- Source secret access key: `minioadmin`
- Destination access key ID: `minioadmin`
- Destination secret access key: `minioadmin`
- Auto-send `/started` for PULL start: checked or unchecked (not used for PUSH)

Click `Start Transfer`, then check the monitor table for `STARTED` and completion progress.

### Example B: Bucket-to-bucket copy (all objects)

- Same as Example A, except:
- Source key: leave empty
- Destination key/prefix: leave empty (it is disabled when source key is empty)

This copies all objects from `source-bucket` to `destination-bucket`, preserving keys.

### Example C: Reverse direction using dataplane-b (MinIO B -> MinIO A)

- Dataplane URL: `http://dataplane-b:8080`
- Transfer mode: `PUSH`
- Source bucket: `destination-bucket`
- Source key: `payload-100mb-copy.bin`
- Destination bucket: `source-bucket`
- Destination key: `payload-return.bin`
- Source endpoint URL: `http://minio-b:9000`
- Destination endpoint URL: `http://minio-a:9000`
- Source/Destination credentials: `minioadmin` / `minioadmin`

### Example D: PULL with automatic `/started`

- Dataplane URL: `http://dataplane-a:8080`
- Transfer mode: `PULL`
- Source bucket: `source-bucket`
- Source key: `payload-100mb.bin`
- Destination bucket: `destination-bucket`
- Destination key: `payload-pull.bin`
- Source endpoint URL: `http://minio-a:9000`
- Destination endpoint URL: `http://minio-b:9000`
- Source/Destination credentials: `minioadmin` / `minioadmin`
- Auto-send `/started` for PULL start: checked (recommended)

## 5) Pause/resume from the UI

From the monitor table:
- Click `Pause` to send `/dataflows/{id}/suspend`.
- Click `Start` to replay start logic for suspended flows.

Note: replay start is available only for flows started by this manual UI instance.

## 6) Verify copied objects

Using MinIO console:
- MinIO B (`http://localhost:19011`) should contain `destination-bucket/payload-100mb-copy.bin`.

Using `mc`:

```bash
mc ls minio-b/destination-bucket
mc stat minio-b/destination-bucket/payload-100mb-copy.bin
```

## 7) Stop the stack

```bash
docker compose -f docker-compose.manual.yml down
```

Remove volumes as well:

```bash
docker compose -f docker-compose.manual.yml down -v
```
