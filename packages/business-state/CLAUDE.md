# Business State

Standalone HTTP business-process state store. **Not a Workflow DevKit World** — no
`Storage`/`Queue`/`Streamer`, not wired up via `WORKFLOW_TARGET_WORLD`. See the package
[README](README.md) for the full picture; this file covers implementation notes only.

## Layout

- `src/store.ts` — DynamoDB access (single table, PK/SK + sparse GSI1 keyed by `runId`)
- `src/handler.ts` — Lambda handler behind API Gateway proxy integration; `createHandler(store)`
  is the testable factory, `handler` is the Lambda entry point (lazily resolves the store from
  `BUSINESS_STATE_TABLE_NAME`)
- `src/auth.ts` — resolves `applicationId` from the caller's API key id via
  `BUSINESS_STATE_APP_KEY_MAP` (baked into the Lambda's environment at CDK synth time — never
  client-supplied)
- `infra/` — separate CDK app/package with its own deploy lifecycle (see `infra/README.md`)

## Commands

```bash
pnpm build          # tsc
pnpm test           # requires Docker (LocalStack via testcontainers)
pnpm typecheck      # tsc --noEmit
```

## Key implementation notes

- **Tenancy lives in the key, not a check.** `PK = APP#<applicationId>#SUB#<subjectType>#<subjectId>`
  — a `GetCommand`/`QueryCommand` scoped to the wrong `applicationId` simply can't return another
  app's item. The one place this isn't automatic is `getStatesByRunId` (GSI1 is keyed by `runId`
  alone) — it filters results by `applicationId` after the query, and the handler turns a
  cross-tenant hit into a plain 404 (not 403 — don't leak existence).
- **PUT is a full replace**, not a partial patch. Omitting `status`/`runId`/`metadata` on a write
  clears them (see `putState`'s `removeParts` branches) — mirrors how `createdAt` is set once via
  `if_not_exists` while every other field is fully overwritten.
- **Lambda bundles from TS source**, not a pre-built `dist/`. `infra/src/lib/business-state-stack.ts`
  points `NodejsFunction` at `../../../src/handler.ts` directly (esbuild handles the bundling) —
  infra deploys stay in sync with source without a separate publish step.

## Common issues

| Issue | Cause / fix |
|-------|-------------|
| 403 on every request | `apiKeyId` not in `BUSINESS_STATE_APP_KEY_MAP` — check the CDK `apps` context param included this app, and that the key was actually sent in `x-api-key`. |
| Cross-tenant runId lookup returns 404 | Expected — see tenancy note above. Not a bug. |
| `NodejsFunction` synth fails needing Docker | `esbuild` isn't resolvable from `infra/`'s node_modules — check it's listed in `infra/package.json` devDependencies and installed. |
