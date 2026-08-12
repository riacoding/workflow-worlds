# @workflow-worlds/business-state

A standalone HTTP store for **current business-process state** — "user X is at step
`CONNECT_CALENDAR`, status `WAITING_FOR_USER`" — so application domain models don't need bespoke
workflow-progress columns.

This is **not** a Workflow DevKit `World`. It doesn't implement `Storage`/`Queue`/`Streamer`, and
it isn't wired up via `WORKFLOW_TARGET_WORLD`. It's a small Lambda + DynamoDB service, deployed
**once** per organization (see [`infra/`](infra)) and shared across multiple apps over HTTP with an
API key. The only thing it optionally knows about a workflow run is its `runId`, kept purely as a
cross-reference for looking state up by run.

## Data model

A **subject** (e.g. a user, an account) can be in several independent **processes** at once (e.g.
`onboarding` and `billing_dispute` simultaneously). Each `(subject, process)` pair has one current
state record:

```ts
interface StateRecord {
  applicationId: string;  // resolved server-side from the caller's API key — never client-supplied
  subjectType: string;    // e.g. "user"
  subjectId: string;
  process: string;        // e.g. "onboarding"
  state: string;          // free-form, e.g. "CONNECT_CALENDAR"
  status?: string;        // free-form, e.g. "WAITING_FOR_USER"
  runId?: string;         // optional cross-reference to a workflow run
  metadata?: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}
```

`state`/`status` are unvalidated free-form strings in v1 — this package doesn't know or care about
your workflow's state machine, it just stores what you send it.

## API

| Method | Path | |
|---|---|---|
| `PUT` | `/state/{subjectType}/{subjectId}/{process}` | Upsert state (full replace of `state`/`status`/`runId`/`metadata`) |
| `GET` | `/state/{subjectType}/{subjectId}/{process}` | Read one process's state |
| `GET` | `/state/{subjectType}/{subjectId}` | List all processes for a subject |
| `GET` | `/runs/{runId}/state` | Look up state by workflow run id |

All routes require an `x-api-key` header. `applicationId` is resolved server-side from that key —
it is never accepted from the request body, so one app can never read or write another's state.

## Usage from a workflow step

Not an importable helper — copy this into your app and wrap it in `'use step'` so it participates
in normal step retry semantics:

```ts
async function recordBusinessState(input: {
  subjectType: string;
  subjectId: string;
  process: string;
  state: string;
  status?: string;
  runId?: string;
  metadata?: Record<string, unknown>;
}) {
  'use step';
  // The deployed ApiUrl output ends with a trailing slash (e.g. ".../prod/") —
  // strip it so this doesn't silently produce a double slash the API won't route.
  const base = process.env.BUSINESS_STATE_API_URL!.replace(/\/$/, '');
  const res = await fetch(
    `${base}/state/${encodeURIComponent(input.subjectType)}/${encodeURIComponent(input.subjectId)}/${encodeURIComponent(input.process)}`,
    {
      method: 'PUT',
      headers: {
        'content-type': 'application/json',
        'x-api-key': process.env.BUSINESS_STATE_API_KEY!,
      },
      body: JSON.stringify({
        state: input.state,
        status: input.status,
        runId: input.runId,
        metadata: input.metadata,
      }),
    }
  );
  if (!res.ok) throw new Error(`business-state write failed: ${res.status}`);
}
```

Call it at the point in your workflow where the user-facing state actually changes; read it back
outside the workflow (e.g. in a route handler rendering onboarding UI) via `GET
/state/{subjectType}/{subjectId}`.

## Deploying

See [`infra/README.md`](infra/README.md) — this is a separate CDK app with its own deploy
lifecycle, independent of any World's infrastructure.

## Development

```bash
pnpm build          # tsc
pnpm test           # requires Docker (LocalStack via testcontainers)
pnpm typecheck      # tsc --noEmit
```

Debug logging: `WORKFLOW_DEBUG=business-state` (writes to stderr; see the repo's debug-logging
convention in the root `CLAUDE.md`).
