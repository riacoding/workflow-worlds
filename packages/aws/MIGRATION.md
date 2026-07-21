# Migration: beta → stable `@workflow/*` (4.1.x-beta → 4.2.x/4.1.x stable)

Audience: an LLM (or engineer) implementing this migration with no other context. Every fix below
is concrete enough to apply directly. This document does **not** change AWS infrastructure
(`packages/aws/infra`, the CDK project) — see "Schema changes" for the one explicit exception
check, which concludes none are needed.

## 1. Overview

`packages/aws` (`@workflow-worlds/aws`) is pinned to beta releases of the Workflow DevKit World
SDK. Target: move to the stable line.

| Package | Declared today (package.json / lockfile) | Target stable |
|---|---|---|
| `@workflow/world` | `4.1.0-beta.2` | `4.2.1` |
| `@workflow/errors` | `4.1.0-beta.14` | `4.1.4` |
| `@workflow/world-testing` (devDep) | `4.1.0-beta.54` | `4.1.11` |

**Drift warning:** at the time this report was written, `node_modules` in this workspace had
already resolved every package's `@workflow/world` / `@workflow/errors` / `@workflow/world-testing`
to the *stable* versions above, even though `package.json` and `pnpm-lock.yaml` still declared the
old betas. That inconsistency (stale manifest, fresher install) can make local `pnpm build` runs
look like they're passing or failing somewhat at random depending on whether `node_modules` gets
reinstalled. Don't treat "it built for me" as a signal either way — go by the manifest/lockfile
state and re-run `pnpm install` after making the edits below.

## 2. Breaking changes summary

| # | Package | Symbol | Beta shape | Stable shape | Severity |
|---|---|---|---|---|---|
| 1 | `@workflow/errors` | `WorkflowAPIError` | class `WorkflowAPIError` | **renamed** to `WorkflowWorldError` (same constructor + new optional `retryAfter?: number`); old name does not exist | **Compile-breaking** — 27 call sites in `storage.ts` |
| 2 | `@workflow/world` | `Streamer.writeToStream` / `closeStream` | `runId: string \| Promise<string>` | `runId: string` | Type-breaking (narrower param) |
| 3 | `@workflow/world` | `Streamer.getStreamChunks` / `getStreamInfo` | did not exist | new **required** methods | **Interface-breaking** — not implemented today |
| 4 | `@workflow/world` | `Storage.events.get` | did not exist | new **required** method | **Interface-breaking** — not implemented today |
| 5 | `@workflow/world` | `Queue.queue()` return | `Promise<{ messageId: MessageId }>` | `Promise<{ messageId: MessageId \| null }>` | Type-widening only — already compatible, no code change |
| 6 | `@workflow/world-testing` | peer `vitest` | `^3.2.4` | `^4.0.18` | devDependency bump required in `packages/aws` and `packages/testing` |
| 7 | `@workflow/world` | dependency `ulid` | not a dependency | `~3.0.1` (hard dep) | Non-breaking; recommended alignment in `packages/aws` |

`Storage.events.create(runId: string | null, ...)` allowing a client-supplied `runId` for
`run_created` is **already handled correctly** in the current code
(`packages/aws/src/storage.ts:541-552`) — no change needed there.

## 3. File-by-file fixes

### 3.1 `packages/aws/package.json`

```diff
   "dependencies": {
     "@aws-sdk/client-dynamodb": "^3.700.0",
     "@aws-sdk/client-scheduler": "^3.700.0",
     "@aws-sdk/client-sqs": "^3.700.0",
     "@aws-sdk/lib-dynamodb": "^3.700.0",
     "@testcontainers/localstack": "^10.0.0",
     "@vercel/queue": "^0.0.0-alpha.29",
-    "@workflow/errors": "4.1.0-beta.14",
-    "@workflow/world": "4.1.0-beta.2",
-    "ulid": "^2.3.0",
+    "@workflow/errors": "4.1.4",
+    "@workflow/world": "4.2.1",
+    "ulid": "^3.0.1",
     "zod": "^4.1.11"
   },
   "devDependencies": {
     "@types/node": "^22.19.1",
     "@workflow-worlds/testing": "workspace:*",
-    "@workflow/world-testing": "4.1.0-beta.54",
+    "@workflow/world-testing": "4.1.11",
     "typescript": "^5.7.0",
-    "vitest": "^3.0.0"
+    "vitest": "^4.0.18"
   },
```

### 3.2 `packages/testing/package.json`

This is a required workspace devDependency of `packages/aws`'s test suite
(`@workflow-worlds/testing`, imported by 5 of the 6 files under `packages/aws/test/`). Its own
`dependencies` block pins the same betas, so it must move in lockstep or `packages/aws` won't
build/typecheck against it.

```diff
   "dependencies": {
-    "@workflow/errors": "4.1.0-beta.14",
-    "@workflow/world": "4.1.0-beta.2"
+    "@workflow/errors": "4.1.4",
+    "@workflow/world": "4.2.1"
   },
   "devDependencies": {
     "@types/node": "^22.0.0",
     "typescript": "^5.7.0",
-    "vitest": "^3.0.0"
+    "vitest": "^4.0.18"
   },
   "peerDependencies": {
-    "vitest": ">=2.0.0"
+    "vitest": ">=4.0.0"
   },
```

After both manifest edits, run `pnpm install` at the repo root to reconcile the lockfile (it
currently still lists the beta resolutions even though `node_modules` had already drifted to
stable — see the drift warning in §1).

### 3.3 `packages/aws/src/storage.ts` — rename `WorkflowAPIError` → `WorkflowWorldError`

Mechanical rename, import line + all 27 throw sites (`new WorkflowAPIError(...)`) and the one
`instanceof`-style cast at the `retryAfter` meta-tagging site:

```diff
 import {
   RunNotSupportedError,
-  WorkflowAPIError,
+  WorkflowWorldError,
   WorkflowRunNotFoundError,
 } from '@workflow/errors';
```

Then replace every occurrence of `WorkflowAPIError` in the rest of the file with
`WorkflowWorldError` (find/replace is safe — the symbol is not used for anything else in this
file), including the cast at the `retryAfter` site:

```diff
-          const err = new WorkflowAPIError(
+          const err = new WorkflowWorldError(
             `Cannot start step '${data.correlationId}' before retryAfter`,
             { status: 425 }
           );
-          (err as WorkflowAPIError & { meta?: Record<string, string> }).meta = {
+          (err as WorkflowWorldError & { meta?: Record<string, string> }).meta = {
             stepId: data.correlationId,
             retryAfter: validatedStep.retryAfter.toISOString(),
           };
```

(Optional, recommended but not required to pass tests: switch the `425`-status
`WorkflowWorldError` above to `TooEarlyError` from `@workflow/errors`, which now exists
specifically for this case and takes `{ retryAfter?: number }` in its constructor — cleaner than
tagging `.meta` by hand. Not required because `@workflow/world-testing`'s `errors` suite asserts
on end-to-end retry timing and output shape over HTTP, not on `instanceof` checks against a
specific world-storage error class.)

### 3.4 `packages/aws/src/storage.ts` — add `events.get()`

Add a new method to the `events` object (alongside the existing `create`/`list`/
`listByCorrelationId`). It's a direct `GetCommand` against the existing `PK=RUN#<runId>,
SK=EVENT#<eventId>` key — no schema change, mirrors the existing `getStepById` pattern:

```ts
async get(runId: string, eventId: string, params?: GetEventParams): Promise<Event> {
  const res = await ddb.send(
    new GetCommand({
      TableName: tableName,
      Key: { PK: runPK(runId), SK: eventSK(eventId) },
    })
  );
  const event = readDoc<Event>(res.Item);
  if (!event) {
    throw new WorkflowWorldError(`Event not found: ${eventId}`, { status: 404 });
  }
  return filterEventData(event, params?.resolveData);
},
```

Add `type GetEventParams` to the existing `@workflow/world` import list at the top of the file.

### 3.5 `packages/aws/src/streamer.ts` — narrow `runId` types

```diff
   return {
     async writeToStream(
       name: string,
-      runId: string | Promise<string>,
+      runId: string,
       chunk: string | Uint8Array
     ): Promise<void> {
-      const resolvedRunId = await runId;
-      await registerStream(resolvedRunId, name);
+      await registerStream(runId, name);
```

```diff
     async closeStream(
       name: string,
-      runId: string | Promise<string>
+      runId: string
     ): Promise<void> {
-      const resolvedRunId = await runId;
-      await registerStream(resolvedRunId, name);
+      await registerStream(runId, name);
```

### 3.6 `packages/aws/src/streamer.ts` — add `getStreamChunks()` / `getStreamInfo()`

Both are built on the existing `loadChunks()` helper (same DynamoDB query pattern already used by
`readFromStream`) — no schema change. `loadChunks()` returns chunks in ascending `chunkId` order,
which is already index-equivalent (monotonic ULID ⇒ append order), so a simple array index serves
as the `0-based index` the new types require. `eof` chunks are excluded from `data` but drive
`done`; a `limit`/`cursor` window is applied the same way the file's `paginate()`-style logic does
elsewhere in this codebase.

Add near the bottom of `createStreamer()`, inside the returned object:

```ts
async getStreamChunks(
  name: string,
  _runId: string,
  options?: GetChunksOptions
): Promise<StreamChunksResponse> {
  const all = await loadChunks(name);
  const dataChunks = all.filter((c) => !c.eof);
  const done = all.some((c) => c.eof);

  const limit = options?.limit ?? 100;
  let startIdx = 0;
  if (options?.cursor) {
    const cursorIdx = Number(options.cursor);
    startIdx = Number.isFinite(cursorIdx) ? cursorIdx + 1 : 0;
  }

  const windowed = dataChunks.slice(startIdx, startIdx + limit);
  const hasMore = startIdx + limit < dataChunks.length;

  return {
    data: windowed.map((chunk, i) => ({
      index: startIdx + i,
      data: chunk.data,
    })),
    cursor: hasMore ? String(startIdx + windowed.length - 1) : null,
    hasMore,
    done,
  };
},

async getStreamInfo(name: string, _runId: string): Promise<StreamInfoResponse> {
  const all = await loadChunks(name);
  const dataChunks = all.filter((c) => !c.eof);
  const done = all.some((c) => c.eof);
  return {
    tailIndex: dataChunks.length - 1,
    done,
  };
},
```

(`runId` is accepted per the interface but unused in the implementation, matching this file's
existing single-table design where streams are addressed by `name` alone —
`listStreamsByRunId` is the only method that actually keys off `runId`. Prefix the param with `_`
to satisfy lint/noUnusedParameters, consistent with the rest of the file's style.)

Update the `import type { Streamer } from '@workflow/world';` line to also pull in the new types:

```diff
-import type { Streamer } from '@workflow/world';
+import type {
+  GetChunksOptions,
+  Streamer,
+  StreamChunksResponse,
+  StreamInfoResponse,
+} from '@workflow/world';
```

### 3.7 `packages/aws/src/queue.ts` — no change required

`Queue.queue()`'s return type widened to `Promise<{ messageId: MessageId | null }>` in the stable
interface. The current implementation always returns a non-null `messageId`
(`packages/aws/src/queue.ts:446-489`), which remains structurally assignable to the wider type via
return-type covariance — TypeScript accepts a function returning a narrower type where a wider one
is expected. No edit needed; noted here only so this isn't mistaken for an unaddressed gap.

### 3.8 `packages/aws/CLAUDE.md` — update error-handling doc

```diff
 ## Error handling

-Use `WorkflowAPIError` from `@workflow/errors` (404 not found, 409 conflict) — never plain
+Use `WorkflowWorldError` from `@workflow/errors` (404 not found, 409 conflict) — never plain
 `Error` (yields a generic 500).
```

## 4. Schema changes

**None required.** `packages/aws/infra` (the CDK project) does not need to change.

Justification: the two new `Streamer` methods (`getStreamChunks`, `getStreamInfo`) are fully
implementable from the existing single-table DynamoDB design already in place
(`PK=STREAM#<name>`, `SK=CHUNK#<ulid>`, each item already carrying `chunkId`/`data`/`eof`) via the
same `loadChunks()` query the file already uses for `readFromStream` — see §3.6. The new
`Storage.events.get()` method is a trivial `GetCommand` against the existing `PK=RUN#<runId>,
SK=EVENT#<eventId>` key — see §3.4. No new attributes, indexes, or tables are needed. The DynamoDB
table (`PK`/`SK` + `GSI1`/`GSI2`), SQS queue, and EventBridge Scheduler group defined in
`infra/src/lib/workflow-aws-stack.ts` are untouched by this migration.

## 5. Verification

```bash
# 1. Build the workspace testing helper package first (packages/aws/test/*.test.ts import it)
cd packages/testing && pnpm build

# 2. Build + typecheck the aws package
cd ../aws && pnpm build && pnpm typecheck

# 3. Run the full test suite (requires Docker — LocalStack via testcontainers)
pnpm test
```

`pnpm test` must pass both:

- The 5 `@workflow/world-testing` suites (via `test/spec.test.ts`'s `createTestSuite()`):
  **addition, idempotency, hooks, errors, nullByte**.
- The package's own `@workflow-worlds/testing` contract tests, which exercise the
  changed/added surface directly: `event-sourcing.test.ts`, `hooks.test.ts`,
  `output-preservation.test.ts`, `serialization.test.ts`, `streamer.test.ts` (the last of these
  will newly exercise `getStreamChunks`/`getStreamInfo` once `@workflow-worlds/testing` is
  updated to call them — if it doesn't yet, that's expected to lag until `packages/testing` adds
  coverage, not a sign this migration is incomplete).

## 5.1 Addendum: gaps found only when actually building

Two fixes were needed beyond §3 that only surfaced once `tsc` was run — noted here because they
weren't visible from the `.d.ts` diff alone (they're consequences of this package's own
delegation-layer/test-helper code, not of the `@workflow/*` interfaces):

- **`packages/aws/src/index.ts`** builds the `World` object as a thin delegation layer that
  forwards each method to the underlying `storage`/`streamer` instance (see `ensureInitialized()`
  and the returned object). It needed two additions to satisfy the now-larger `World` interface:
  - `events.get(runId, eventId, params)` passthrough to `storage.events.get(...)`, alongside the
    existing `create`/`list`/`listByCorrelationId` passthroughs.
  - `getStreamChunks(name, runId, options)` and `getStreamInfo(name, runId)` passthroughs to the
    `streamer` instance, alongside the existing `writeToStream`/`closeStream`/`readFromStream`/
    `listStreamsByRunId` passthroughs.
- **`packages/testing/src/queue.ts`** (the `queueTests()` contract-test helper) asserted
  `result.messageId.startsWith('msg_')` directly, which no longer typechecks now that
  `Queue.queue()` returns `messageId: MessageId | null`. Fixed with an explicit
  `expect(result.messageId).not.toBeNull()` runtime check followed by a `!` non-null assertion —
  `packages/aws`'s own `queue.ts` implementation always returns a non-null `messageId` (§3.7), so
  the assertion is safe for this world, but this test helper is shared across all world packages
  and each one would need the same tolerance if any of them ever legitimately returned `null`.

## 6. Out of scope

`packages/mongodb`, `packages/redis`, `packages/turso`, `packages/starter`, and `workbench` all
carry the identical beta pins (`@workflow/errors@4.1.0-beta.14`, `@workflow/world@4.1.0-beta.2`,
`@workflow/world-testing@4.1.0-beta.54`) and the identical `node_modules`/manifest drift described
in §1. None are touched by this migration. The same fix pattern in this document (§3.3 rename,
§3.4/§3.6 new required methods, §3.1-style manifest bumps) applies directly if/when those packages
are migrated — `storage.ts` in each has the same `WorkflowAPIError` import shape (mongodb ~20 call
sites, redis ~25, turso ~20, starter ~14), and each package's own `streamer.ts` will need the same
two new methods.
