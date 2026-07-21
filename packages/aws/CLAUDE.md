# AWS World

AWS-backed World implementation for the Workflow DevKit.

- **Storage** — DynamoDB single-table (`src/storage.ts`)
- **Queue** — SQS + DynamoDB TTL idempotency (`src/queue.ts`)
- **Scheduling** — EventBridge Scheduler for long delays (`src/queue.ts`)
- **Streamer** — DynamoDB persistence + AppSync Events publish (`src/streamer.ts`)
- **Shared** — clients, config resolution, provisioning (`src/aws.ts`)

## Commands

```bash
pnpm build          # tsc
pnpm test           # requires Docker (LocalStack via testcontainers)
pnpm test:only      # run tests without rebuilding the package
pnpm typecheck      # tsc --noEmit
```

## Key implementation notes

- **Single table, single `doc` attribute.** Each item stores query keys (`PK`/`SK`/GSI keys,
  `entity`, `status`, `workflowName`, `correlationId`) natively and the full domain object as a
  JSON string in `doc`. The codec in `src/utils.ts` (`encodeJson`/`decodeJson`) tags `Date` and
  `Uint8Array` so they round-trip — the core stores run/step `input`/`output` as `Uint8Array`
  (devalue binary) and relies on `Date` methods. Plain `JSON.parse(JSON.stringify(...))` breaks
  both.
- **Event ordering.** `events.list` returns oldest-first; `EVENT#<eventId>` with monotonic ULIDs
  sorts chronologically.
- **Idempotency.** `queue()` reserves `IDEMPOTENCY#<key>` with a conditional write and a 5s TTL
  window (network-retry dedupe only — never inflight tracking).
- **Worker robustness.** Short SQS visibility timeout + a heartbeat that extends it while
  processing, plus `SIGTERM`/`SIGINT` release of in-flight messages. This is what makes multiple
  short-lived worker processes (as in the test harness) share one queue without stalling on the
  visibility window.

## Common issues

| Issue | Cause / fix |
|-------|-------------|
| `Invalid input` in `unflatten` / run stuck `pending` | `Uint8Array` input/output not preserved — the `doc` codec must tag binary. |
| `step.retryAfter.getTime is not a function` | Date not preserved — use the codec / `structuredClone`, never JSON round-trip. |
| Non-first integration test stalls ~visibility timeout | A killed worker holds a message invisible; keep the short visibility + heartbeat + graceful release. |
| Tests can't reach AWS | Docker not running (LocalStack), or set `WORKFLOW_AWS_ENDPOINT` to an existing endpoint. |

## Error handling

Use `WorkflowWorldError` from `@workflow/errors` (404 not found, 409 conflict) — never plain
`Error` (yields a generic 500).
