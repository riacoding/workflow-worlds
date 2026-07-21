# Bench and E2E Scripts: How They Work

This document explains the mechanics of the two cross-package test/measurement flows in this
repo — `pnpm bench:<world>` and `pnpm e2e:<world>` — and how the newly-added `aws` world was
wired into each. It is descriptive, not a how-to: for usage instructions see
[`workbench/BENCHMARKS.md`](../workbench/BENCHMARKS.md) and the header comment in
[`scripts/e2e-upstream.sh`](../scripts/e2e-upstream.sh).

Both flows exist because a World implementation (`packages/{world}`) is a library, not a runnable
app — it needs a host process that actually calls `start()`, serves the `.well-known/workflow/v1/*`
routes, and lets you trigger real workflow runs against it. `workbench/` (an in-repo Nitro app) and
`.e2e-upstream/` (a cloned copy of `vercel/workflow`'s own Next.js example app) are the two hosts
used for that, for two different purposes: **bench** measures performance of *this repo's* worlds
against a minimal, purpose-built app; **e2e** validates compatibility against the *upstream
framework's own* example app and test suite.

## 1. `pnpm bench:<world>`

### 1.1 Entry point and world selection

```
pnpm bench:aws
  → root package.json: "bench:aws": "WORKFLOW_TARGET_WORLD=@workflow-worlds/aws pnpm bench"
  → root package.json: "bench": "pnpm --filter @workflow-worlds/workbench bench"
  → workbench/package.json: "bench": "bash scripts/run-bench.sh"
```

`WORKFLOW_TARGET_WORLD` is the only thing that varies per world — it's an env var read at runtime
by `@workflow/core`'s `createWorld()` (inside the `workflow` package `workbench` depends on), which
does roughly:

```js
const targetWorld = process.env.WORKFLOW_TARGET_WORLD || defaultWorld();
// ... 'vercel' / 'local' special-cased, otherwise:
const mod = require(targetWorld);        // e.g. require('@workflow-worlds/aws')
return mod.default();                     // the World factory, per the CORRECT/WRONG export
                                           // convention documented in the root CLAUDE.md
```

Because this is a plain Node `require()` resolved from the workbench process's own module
resolution, **the target world package must be an installed dependency of `workbench/`** — not
just present elsewhere in the workspace. This is why `workbench/package.json` lists
`@workflow-worlds/turso`, `@workflow-worlds/mongodb`, `@workflow-worlds/redis`, and
`@workflow-worlds/starter` as `devDependencies`. `@workflow-worlds/aws` has now been added to that
same list for `bench:aws` to be able to resolve it — without it, `require('@workflow-worlds/aws')`
would throw `MODULE_NOT_FOUND` the first time the workbench process touches the world (lazily, on
first request, since nothing eagerly imports it at build time).

### 1.2 `scripts/run-bench.sh` — building and starting the host

1. **Redis-only pre-step**: if `WORKFLOW_TARGET_WORLD` contains `redis`, `FLUSHALL`s the configured
   Redis instance over a raw `nc` connection, to avoid stale jobs from a previous run skewing
   timings. No equivalent pre-step exists (or is needed) for `aws`.
2. **`pnpm build`** — this is `workbench`'s own `nitro build` (via its `build` script), *not* a
   rebuild of the target world package. It runs `prebuild` first
   (`generate-workflows-registry.js`, see §1.4), then Nitro bundles the app into
   `workbench/.output/server/`. The world package itself must already be built
   (`packages/{world}/dist/`) since `nitro build` only bundles what's importable — this is a gap to
   watch for with `aws` specifically, since it isn't in the top-level `turbo run build` filter list
   the same way (see §1.6 caveat).
3. **Start the server**: `node .output/server/index.mjs &`, capturing `$SERVER_PID`, with a `trap
   cleanup EXIT` that kills it on script exit (success or failure).
4. **World startup**: Nitro's plugin system runs `workbench/plugins/start-world.ts` on server
   init, which calls `getWorld()` (creating/caching the World singleton — this is the `require()`
   call from §1.1) and then `world.start()` if the World defines it. For `@workflow-worlds/aws`,
   `start()` (`packages/aws/src/index.ts`) triggers `ensureInitialized()`, which — if
   `WORKFLOW_AWS_LOCAL=true` is set — calls `startLocalStack()`
   (`packages/aws/src/local.ts`) to spin up a LocalStack container via `testcontainers` *before*
   the server can serve DynamoDB/SQS-backed requests. This means the very first request against an
   `aws` bench run can be slow (container cold start), which the `curl` poll loop in the next step
   accounts for by retrying for up to 30 seconds.
5. **Readiness poll**: polls `http://localhost:3000` every second, up to 30 attempts, before
   proceeding.
6. **Run the benchmarks**: `WORLD_NAME` is derived by stripping the `@workflow-worlds/` prefix off
   `WORKFLOW_TARGET_WORLD` (so `aws` for `@workflow-worlds/aws`), then:
   ```bash
   DEPLOYMENT_URL=http://localhost:3000 WORLD_NAME=aws \
     pnpm exec vitest bench --run --outputJson=bench-results-aws.json
   ```

### 1.3 `workbench/test/bench.bench.ts` — what actually gets measured

This is a `vitest bench` suite (not a normal test file) with 5 named benchmarks (no-step,
1-step, 10-sequential-steps, 10-parallel-steps, streaming). Each benchmark iteration:

1. Calls `triggerWorkflow(workflowFn, args)`, which does `POST /api/trigger?workflowFile=...&workflowFn=...`
   with the args JSON-encoded in the body. This hits `workbench/routes/api/trigger.post.ts`, which
   looks up the named workflow function in the auto-generated `_workflows.ts` registry (§1.4) and
   calls `start(workflow, args)` from `workflow/api` — this is what actually calls into the World's
   `queue()`/`events.create()` machinery to create and dispatch the run.
2. Polls `GET /api/trigger?runId=...` (`trigger.get.ts`) until the run resolves — this endpoint
   returns HTTP 202 while `getRun(runId).returnValue` is still pending (mapped from
   `WorkflowRunNotCompletedError`), and otherwise returns the resolved value plus
   `X-Workflow-Run-{Created,Started,Completed}-At` headers sourced straight from the World's
   `runs.get()`.
3. Records timing via `stageTiming()`, buffered per-iteration and flushed in vitest's `teardown`
   hook once each bench "run" phase (as opposed to its warmup phase) completes. `executionTimeMs`
   is `completedAt - createdAt` from those headers — i.e. it's timing what the *World* reports for
   itself, not wall-clock time measured by the bench harness, so it reflects each World's own
   understanding of when a run started/finished.
4. For the streaming benchmark specifically, it also measures `firstByteTimeMs` — time from the
   run's reported `startedAt` to when the first non-empty chunk is readable from the returned
   `ReadableStream`, which for `aws` exercises `Streamer.writeToStream`/`readFromStream`
   end-to-end (including the DynamoDB persistence + in-process `EventEmitter` fan-out described in
   `packages/aws/src/streamer.ts`).

Output: `bench-results-{world}.json` (raw vitest bench output — ops/sec, iteration counts) and
`bench-timings-{world}.json` (the `workflowTimings`/`summary` structure built by `bench.bench.ts`
itself — avg/min/max execution and first-byte times per benchmark name), both written to
`workbench/`.

### 1.4 Workflow registry generation

`workbench/workflows/bench.ts` defines the actual workflow/step functions being benchmarked
(`noStepsWorkflow`, `oneStepWorkflow`, `tenSequentialStepsWorkflow`, `tenParallelStepsWorkflow`,
`streamWorkflow` — using the `'use workflow'`/`'use step'` directive convention the Workflow DevKit
compiler looks for). `workbench/scripts/generate-workflows-registry.js` runs as a `predev`/`prebuild`
hook, scanning `workflows/*.ts` and emitting `workbench/_workflows.ts`: a static import map
(`allWorkflows['workflows/bench.ts'] = { noStepsWorkflow, ... }`) that `trigger.post.ts` looks
workflow functions up in by file+function name from the request query string. This file is
regenerated on every build, not hand-edited — it's how new benchmark workflows become
addressable over HTTP without a bespoke route per workflow.

### 1.5 Nitro build configuration

`workbench/nitro.config.ts` loads the `workflow/nitro` module (the framework's own build
integration — this is what makes `'use workflow'`/`'use step'` directives get compiled into
`.well-known/workflow/v1/{flow,step}/route.js` handlers) and registers `start-world.ts` as a
startup plugin (§1.2 step 4). It also patches Rollup's tree-shaking to preserve module side effects
for anything under `.nitro/workflow/` or `workflow/internal/private` — without this, Rollup would
strip the step-registration side effects that the generated step/flow bundles rely on, since they
look like unused imports from a pure dependency-graph perspective.

### 1.6 Caveat: this analysis did not execute anything

Per the request, none of `bench:aws`, `e2e:aws`, `pnpm install`, or `pnpm build` were run to verify
this end-to-end. Two things worth checking before the first real `bench:aws` run:

- `packages/aws` must be built (`pnpm --filter @workflow-worlds/aws build`) so `dist/` exists for
  Nitro/`require()` to resolve — same requirement as any other world, just flagged explicitly since
  it wasn't exercised here.
- `WORKFLOW_AWS_LOCAL=true` (or a pre-existing `WORKFLOW_AWS_ENDPOINT`) needs to be exported before
  running `bench:aws`, exactly as `WORKFLOW_REDIS_URI`/`WORKFLOW_MONGODB_URI` need to be reachable
  before `bench:redis`/`bench:mongodb` — the bench script itself sets no world-specific env vars
  (see `workbench/BENCHMARKS.md`'s "Prerequisites" section, now updated with an `aws` row).

## 2. `pnpm e2e:<world>`

### 2.1 Entry point

```
pnpm e2e:aws
  → root package.json: "e2e:aws": "./scripts/e2e-upstream.sh aws"
```

Unlike bench (which runs an in-repo app), e2e validates a world package against the **actual
upstream `vercel/workflow` repository's own example app and test suite** — the goal is "does this
world implementation work correctly when driven by the real framework's own e2e tests," which is a
stronger compatibility signal than this repo's own contract tests (`@workflow/world-testing`,
`@workflow-worlds/testing`) provide on their own, since those are written by world authors and
could share blind spots with the implementation.

### 2.2 World configuration tables

`scripts/e2e-upstream.sh` keys almost everything world-specific off five bash associative arrays,
indexed by a short world id (`starter`, `turso`, `mongodb`, `redis`, and now `aws`):

| Array | Purpose | `aws` value |
|---|---|---|
| `WORLD_PACKAGE` | npm package name to build/pack/install | `@workflow-worlds/aws` |
| `WORLD_LOCAL_DIR` | local path to build from | `packages/aws` |
| `WORLD_SERVICE` | which Docker service the script itself must start | `none` (see below) |
| `WORLD_SETUP` | extra setup command run inside the upstream app dir | *(empty)* |
| `WORLD_ENV` | env vars exported before the upstream dev server starts | `WORKFLOW_TARGET_WORLD=@workflow-worlds/aws`, `WORKFLOW_AWS_LOCAL=true` |

**Design note on `WORLD_SERVICE[aws]=none`:** for `mongodb`/`redis`, the script owns the
service's entire lifecycle — `start_mongodb`/`start_redis` functions `docker run -d --name
e2e-mongodb ...`, poll for readiness, and the `cleanup()` trap explicitly `docker stop`/`docker rm`s
those named containers on exit. `aws` deliberately does *not* follow that pattern: the world package
already has a purpose-built `WORKFLOW_AWS_LOCAL=true` feature
(`packages/aws/src/local.ts`) that starts a LocalStack container via `testcontainers` internally,
including its own `SIGINT`/`SIGTERM` shutdown handling and (via testcontainers' Ryuk reaper
sidecar) automatic cleanup even on unclean exit. Duplicating that lifecycle management in bash
would be redundant and drift-prone, so the script simply sets `WORKFLOW_AWS_LOCAL=true` in
`WORLD_ENV[aws]` and lets the world manage it. The practical consequence: `WORLD_SERVICE[aws]=none`
means the script's own "Step 4: Starting Docker services" phase is a no-op for `aws` (it logs "No
Docker services needed" and skips the `command -v docker` check that phase would otherwise run) —
but Docker itself is still a hard runtime requirement, just satisfied transitively by
`testcontainers` when the upstream app's dev server first touches the World. If Docker isn't
running, the failure will surface later and less clearly (inside the upstream Next.js dev server's
logs) than it would for `mongodb`/`redis`, where the script fails fast with an explicit error.

### 2.3 The 8 steps (from the script's own section banners)

1. **Build local world package**: `pnpm build --filter="@workflow-worlds/aws..."` (builds the
   world and its local workspace dependencies — for `aws` this pulls in
   `@workflow-worlds/testing` transitively the same way `packages/aws`'s own `pnpm test` does),
   then `pnpm pack` inside `packages/aws` to produce a `.tgz` tarball in
   `.e2e-upstream-tarballs/`. Packing (rather than a workspace symlink) is what lets the tarball be
   installed cleanly into a completely separate git clone in the next step, with no
   pnpm-workspace/monorepo assumptions leaking across repo boundaries.
2. **Clone or update upstream**: shallow-clones (`--depth 1`) `github.com/vercel/workflow` into
   `.e2e-upstream/` (or `--clean`s and re-clones; or fetches+checks out `FETCH_HEAD` if already
   cloned) at the `main` ref by default (`E2E_UPSTREAM_REF`).
3. **Install + build upstream**: rewrites every nested `package.json`'s `packageManager` field to
   match the local pnpm version (avoids corepack trying to download a pinned version in restricted
   network environments), `pnpm install --no-frozen-lockfile`, then `pnpm add --workspace-root
   <tarball>` (registers the packed world as an upstream workspace dependency). If
   `E2E_SKIP_BUILD` isn't set, it also works around the upstream SWC plugin's native Rust/WASM build
   requirement by fetching the pre-built `.wasm` from the already-published npm package instead of
   compiling from source, then runs `pnpm turbo run build` across the upstream monorepo (excluding
   its own workbenches/docs), tolerating non-critical build failures via `--continue` as long as a
   fixed list of critical packages (`core`, `next`, `builders`, `errors`, `serde`, `utils`, `world`,
   `cli`) end up with a `dist/`/`build/` output.
4. **Start Docker services**: per `WORLD_SERVICE[$WORLD_ID]` — a no-op for `aws` (§2.2).
5. **Install into upstream workbench**: `pnpm --filter nextjs-turbopack add <tarball-path>` inside
   the cloned upstream repo — `nextjs-turbopack` is the default `E2E_APP_NAME`, one of the upstream
   repo's own example apps used as the e2e host (the analogue of this repo's `workbench/`, but
   owned by upstream). Then runs `WORLD_SETUP[$WORLD_ID]` if non-empty (only `turso` needs one, to
   provision its SQLite file via `workflow-turso-setup`; `aws` needs none since LocalStack
   provisioning is handled by `WORKFLOW_AWS_LOCAL`).
6. **Resolve symlinks**: runs the upstream repo's own `scripts/resolve-symlinks.sh` if present,
   which (based on its usage here) flattens pnpm workspace symlinks inside the target app so a
   plain `node_modules` resolution behaves correctly outside the monorepo — necessary because step 3
   just turned the packed world tarball into a real (non-symlinked) dependency, and other upstream
   internal deps may still be symlinked in ways that assume the full monorepo context.
7. **Set environment variables**: exports everything from `WORLD_ENV[$WORLD_ID]` (§2.2) plus fixed
   env vars: `DEPLOYMENT_URL`/`WORKFLOW_SERVICE_URL=http://localhost:3000`, `APP_NAME`,
   `NODE_OPTIONS=--enable-source-maps`, `WORKFLOW_PUBLIC_MANIFEST=1`, and a `DEV_TEST_CONFIG` JSON
   blob describing file paths the (optional) dev/HMR test suite needs to locate generated routes.
8. **Start dev server and run tests**: `cd`s into the upstream app dir, backgrounds `pnpm dev`
   (capturing `$DEV_SERVER_PID` for the cleanup trap), waits up to 90s
   (`wait_for_server`, 45 attempts × 2s) for `http://localhost:3000` to answer with any of
   `200`/`404`/`500` (i.e. "a server is listening," not "the app is fully healthy" — 404/500 still
   count because Next.js can 404 the root route validly), sleeps an extra 5s buffer, then runs
   `packages/core/e2e/e2e.test.ts` from the **upstream repo's own test suite** via
   `vitest run --config vitest.config.ts` (also upstream's config, not this repo's). This is the
   crux of the "does the real framework agree this world works" validation — the tests themselves
   are entirely out of this repo's control, which is the point. `--dev-tests` optionally also runs
   `packages/core/e2e/dev.test.ts` (HMR-focused) after an additional 10s buffer.

Exit code is the worse of the two vitest runs (`E2E_EXIT_CODE`); `cleanup()` (registered via `trap
... EXIT` near the top of the script) always runs on the way out regardless of success/failure,
killing the dev server and — for `mongodb`/`redis` only, per §2.2 — stopping their Docker
containers.

### 2.4 CI aggregation: `scripts/aggregate-e2e-results.cjs`

Not invoked by `e2e-upstream.sh` itself — this is a separate script (presumably wired into a CI
workflow not covered by this analysis) that scans a results directory for `e2e-{world}.json` files
(vitest's own JSON reporter output, one per world, e.g. produced by running e2e for every world in
a CI matrix and collecting the outputs), parses pass/fail/skip counts per file via
`content.testResults[].assertionResults[].status`, and renders a single markdown summary table
(`e2e-summary.md`) — sorted failed-worlds-first, with a `<details>`-collapsed list of failure
messages per world, suitable for posting as a PR comment. `worldFromFilename()` derives the world id
from the filename convention `e2e-<world>.json`, so once CI actually produces an `e2e-aws.json`
(by running `e2e:aws` with a JSON reporter output path matching that convention), it will show up
in the aggregate table automatically — the aggregator has no per-world hardcoding to update, except
its optional `worldNames` display-name map (`turso`/`mongodb`/`redis`/`starter` → capitalized
names; an unmapped id like `aws` just falls back to the raw id as the display name, which is a
cosmetic-only gap, not a functional one).

## 3. Summary: what changed to add `aws` support

| File | Change |
|---|---|
| `package.json` (root) | Added `bench:aws` and `e2e:aws` scripts, mirroring the existing per-world pattern |
| `workbench/package.json` | Added `@workflow-worlds/aws` as a `devDependency` — required for `require('@workflow-worlds/aws')` to resolve inside the bench host process |
| `scripts/e2e-upstream.sh` | Added `aws` entries to `WORLD_PACKAGE`/`WORLD_LOCAL_DIR`/`WORLD_SERVICE`/`WORLD_SETUP`/`WORLD_ENV`, and to the `usage()` help text |
| `workbench/BENCHMARKS.md` | Documented `aws` prerequisites/env vars and added it to the world configuration table |

No script in this list was executed as part of this change — see §1.6 for what to verify before the
first real run.
