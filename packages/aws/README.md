# @workflow-worlds/aws

An AWS-native [World](https://github.com/mizzle-dev/workflow-worlds) implementation for the Workflow DevKit, backed entirely by AWS managed services.

| Capability | AWS service |
|------------|-------------|
| **Storage** (runs, steps, events, hooks) | DynamoDB (single-table design) |
| **Queue** (step/workflow dispatch) | SQS (standard queue) + DynamoDB TTL idempotency |
| **Scheduling** (long delays / retry backoff) | EventBridge Scheduler |
| **Streamer** (real-time output) | DynamoDB persistence + AppSync Events API pub/sub |

## Install

```bash
pnpm add @workflow-worlds/aws
```

Then point the runtime at it:

```bash
WORKFLOW_TARGET_WORLD=@workflow-worlds/aws
```

## Configuration

Every option follows the priority **config value > env var > default**. Connection-style
settings use the `WORKFLOW_` prefix.

| Env var | Description | Default |
|---------|-------------|---------|
| `WORKFLOW_AWS_REGION` | AWS region | `us-east-1` |
| `WORKFLOW_AWS_ENDPOINT` | Custom endpoint for all services (LocalStack / emulation) | — |
| `WORKFLOW_AWS_LOCAL` | Auto-start a local LocalStack Docker container (DynamoDB + SQS) and point this World at it — for local dev/inspection without touching real AWS | `false` |
| `WORKFLOW_AWS_ACCESS_KEY_ID` | Static access key (mainly for emulation) | default chain |
| `WORKFLOW_AWS_SECRET_ACCESS_KEY` | Static secret key (mainly for emulation) | default chain |
| `WORKFLOW_DYNAMODB_TABLE_NAME` | Single table name | `workflow` |
| `WORKFLOW_AWS_AUTO_PROVISION` | Auto-create table + queue if missing | `true` |
| `WORKFLOW_SQS_QUEUE_URL` | Full SQS queue URL (skips name lookup) | — |
| `WORKFLOW_SQS_QUEUE_NAME` | SQS queue name (resolves/creates the URL) | `workflow-queue` |
| `WORKFLOW_SQS_QUEUE_ARN` | SQS queue ARN (EventBridge Scheduler target) | resolved |
| `WORKFLOW_SCHEDULER_GROUP_NAME` | EventBridge Scheduler group | `workflow` |
| `WORKFLOW_SCHEDULER_ROLE_ARN` | IAM role the scheduler assumes to deliver to SQS | — |
| `WORKFLOW_APPSYNC_EVENTS_ENDPOINT` | AppSync Events API HTTP endpoint | — |
| `WORKFLOW_APPSYNC_API_KEY` | AppSync Events API key (`x-api-key` auth) | — |
| `WORKFLOW_SERVICE_URL` | Base URL for HTTP callbacks | `http://localhost:{PORT}` |
| `WORKFLOW_CONCURRENCY` | Max concurrent message processing | `20` |
| `WORKFLOW_DEBUG` | Debug logging (`1`, `aws-world`, …) writes to stderr | off |

Credentials resolve through the standard AWS SDK provider chain unless
`WORKFLOW_AWS_ACCESS_KEY_ID` / `WORKFLOW_AWS_SECRET_ACCESS_KEY` are set explicitly.

## DynamoDB single-table design

One table holds all four namespaces via a composite `PK`/`SK` prefix scheme plus two GSIs:

| Entity | `PK` | `SK` | `GSI1PK` / `GSI1SK` | `GSI2PK` / `GSI2SK` |
|--------|------|------|--------------------|---------------------|
| Run    | `RUN#<runId>`  | `RUN#<runId>`     | `RUNLIST` / `<runId>`         | — |
| Step   | `RUN#<runId>`  | `STEP#<stepId>`   | `STEP#<stepId>` / `STEP#<stepId>` | — |
| Event  | `RUN#<runId>`  | `EVENT#<eventId>` | `CORR#<corrId>` / `<eventId>` *(if correlated)* | — |
| Hook   | `RUN#<runId>`  | `HOOK#<hookId>`   | `HOOK#<hookId>` / `HOOK#<hookId>` | `TOKEN#<token>` / `TOKEN#<token>` |

- **Event ordering** is guaranteed oldest-first: `eventId`s are monotonic ULIDs, so the
  `EVENT#<eventId>` sort key orders chronologically for deterministic replay.
- **GSI1** serves: list-all-runs, step lookup without a `runId`, hook lookup by `hookId`,
  and events-by-`correlationId`.
- **GSI2** serves hook lookup by token.
- Conditional writes (`attribute_not_exists`) enforce idempotent step / hook creation.
- Domain objects are stored as a single JSON attribute encoded with a Date- and
  binary-preserving codec, so `Date` methods and `Uint8Array` input/output survive the
  round-trip (never `JSON.parse(JSON.stringify(...))`).

The queue's idempotency records live in the same table under `IDEMPOTENCY#<key>` with a
DynamoDB TTL attribute. Deduplication uses a 5-second window (network-retry dedupe only —
**not** inflight tracking, which deadlocks workflows).

## Queue & worker semantics

- `queue()` reserves the idempotency key with a conditional DynamoDB write, then sends to SQS.
- `start()` launches a long-polling worker that dispatches each message to the workflow
  server's `/.well-known/workflow/v1/{step,flow}` endpoint.
- Messages use a short visibility timeout with a heartbeat that extends it while work is in
  progress, so a message orphaned by a crashed/killed worker is redelivered quickly.
- On `SIGTERM`/`SIGINT` the worker releases in-flight messages for immediate redelivery.
- `503 { timeoutSeconds }` reschedules without counting as a failure; other failures retry
  with exponential backoff (SQS visibility). Delays beyond SQS's 12-hour ceiling are handed
  to **EventBridge Scheduler** as one-off schedules (requires `WORKFLOW_SCHEDULER_ROLE_ARN`
  and `WORKFLOW_SQS_QUEUE_ARN`).

## Streamer

Chunks are persisted in DynamoDB (`STREAM#<name>` / `CHUNK#<ulid>`) for history/replay and
`readFromStream()` resume. Real-time delivery uses an in-process emitter plus an optional
DynamoDB tail-poll for cross-process readers. When `WORKFLOW_APPSYNC_EVENTS_ENDPOINT` is
configured, each chunk is also published to a per-stream AppSync Events channel
(`/streams/<name>`) for external fan-out.

## Running tests locally

Tests run against emulated AWS via [LocalStack](https://localstack.cloud) using
`@testcontainers/localstack`, so **Docker (or a compatible runtime) must be running**.

```bash
pnpm build
pnpm test          # builds the testing pkg, builds this pkg, runs vitest
pnpm test:only     # skip the package rebuild
```

The suite starts a LocalStack container automatically. To run against an already-running
LocalStack (or a real AWS test account), set `WORKFLOW_AWS_ENDPOINT` (and credentials /
region) before running and the container is skipped.

Test suites: the five `@workflow/world-testing` integration suites (addition, idempotency,
hooks, errors, nullByte) plus the `@workflow-worlds/testing` storage/streamer contract tests.

### Notes on emulation coverage

- **DynamoDB** and **SQS** are fully emulated by LocalStack Community.
- **EventBridge Scheduler** is only exercised for delays beyond SQS's 12-hour ceiling, which
  the test suites do not hit. `ensureSchedulerGroup` and schedule creation are best-effort
  and never fail startup.
- **AppSync Events** is publish-only and best-effort; it is not required for tests and is a
  no-op unless `WORKFLOW_APPSYNC_EVENTS_ENDPOINT` is set.

## Local development with Docker

Setting `WORKFLOW_AWS_LOCAL=true` makes the World start its own [LocalStack](https://localstack.cloud)
container (DynamoDB + SQS) on first use and point itself at it automatically — no manual
endpoint, region, or credential setup needed, and nothing touches real AWS. Docker (or a
compatible runtime) must be running. The container is ephemeral: it starts fresh each time the
process starts and stops when the process receives `SIGINT`/`SIGTERM`.

```bash
WORKFLOW_TARGET_WORLD=./dist/index.js
WORKFLOW_AWS_LOCAL=true
WORKFLOW_DEBUG=aws-world   # optional: see container/init logs on stderr
```

### Building, packing, and installing into another project

To try a local build of this package in a separate, already-existing project (rather than
publishing to a registry), pack it into a tarball and install that directly:

```bash
# 1. From packages/aws — build
cd packages/aws
pnpm build

# 2. Pack it into a tarball
pnpm pack --pack-destination /tmp/workflow-aws-tarballs
# -> /tmp/workflow-aws-tarballs/workflow-worlds-aws-0.1.0.tgz

# 3. In the other project — install the tarball
cd /path/to/your-project
pnpm add /tmp/workflow-aws-tarballs/workflow-worlds-aws-0.1.0.tgz
# (npm/yarn equivalents work too — it's a plain tarball install)

# 4. Point the runtime at it and enable local mode
export WORKFLOW_TARGET_WORLD=@workflow-worlds/aws
export WORKFLOW_AWS_LOCAL=true
export WORKFLOW_DEBUG=aws-world   # optional

# 5. Run your project as usual
pnpm dev
```

After any change to `packages/aws/src/*`, repeat steps 1–3 (rebuild, repack, reinstall) — a
tarball install doesn't pick up source changes automatically the way a workspace `link:` would.

To inspect local data, point the AWS CLI at the endpoint printed in the startup banner
(credentials `test`/`test`, region matches `WORKFLOW_AWS_REGION`, default `us-west-2`):

```bash
aws --endpoint-url <endpoint-from-banner> dynamodb scan --table-name workflow
```

## Provisioning for real AWS

With `WORKFLOW_AWS_AUTO_PROVISION=true` (default) the world creates the DynamoDB table and
SQS queue on first use — convenient for dev, but in production you should provision
infrastructure with IaC and set `WORKFLOW_AWS_AUTO_PROVISION=false`.

**DynamoDB table:** `PK` (HASH) + `SK` (RANGE), two GSIs `GSI1` (`GSI1PK`/`GSI1SK`) and
`GSI2` (`GSI2PK`/`GSI2SK`) projecting `ALL`, and TTL enabled on the `ttl` attribute.

**SQS:** one standard queue (a dead-letter redrive policy is recommended in production).

**IAM permissions** the runtime needs:

- DynamoDB: `GetItem`, `PutItem`, `DeleteItem`, `Query`, `Scan`, `UpdateTimeToLive`,
  `DescribeTable` (+ `CreateTable` if auto-provisioning) on the table and its indexes.
- SQS: `SendMessage`, `ReceiveMessage`, `DeleteMessage`, `ChangeMessageVisibility`,
  `GetQueueUrl`, `GetQueueAttributes` (+ `CreateQueue` if auto-provisioning).
- EventBridge Scheduler (optional, for long delays): `CreateSchedule`, `CreateScheduleGroup`,
  plus a role (`WORKFLOW_SCHEDULER_ROLE_ARN`) that grants the scheduler `sqs:SendMessage` to
  the queue.
- AppSync Events (optional): access to publish to the configured Events API.

## License

MIT
