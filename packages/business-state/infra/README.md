# @workflow-worlds/business-state-infra

AWS CDK (TypeScript) infrastructure for [`@workflow-worlds/business-state`](../README.md): a
DynamoDB table, a Lambda function (bundled directly from `../src/handler.ts` via esbuild — no
separate build-and-publish step), and a REST API Gateway with one API key per app.

**Deploying is entirely up to you.** This project doesn't deploy anything on its own — you run
`cdk bootstrap`/`deploy`/`destroy` yourself, whenever you're ready.

## Naming and scope

Unlike [`@workflow-worlds/aws-infra`](../../aws/infra), this stack is meant to be deployed **once,
org-wide** — a single shared instance, not one per project:

- No `projectName` context param.
- `stage` defaults to `prod` (override with `-c stage=...` if you want a separate dev instance).
- `removalPolicy` defaults to **`retain`**, always — not just in prod. This store may become the
  source of truth for business state across every app in your org; tearing it down requires an
  explicit `-c removalPolicy=destroy`.
- `apps` — **required**, no default. Comma-separated app names, one API key minted per app, e.g.
  `-c apps=onboarding,billing`. `cdk synth`/`deploy` fails with a clear error if it's omitted.

## Prerequisites

- An AWS account and credentials configured (e.g. `AWS_PROFILE`, or `aws configure`).
- `cdk bootstrap` run once per account/region (first-time CDK setup only):
  ```bash
  pnpm exec cdk bootstrap
  ```

## Deploy workflow

```bash
# 1. Install (from the monorepo root, or within this directory)
pnpm install

# 2. (Optional) preview the changes
pnpm --filter @workflow-worlds/business-state-infra run diff -c apps=onboarding,billing

# 3. Deploy — writes outputs to cdk-outputs.json
pnpm --filter @workflow-worlds/business-state-infra run deploy -c apps=onboarding,billing

# 4. Print the API URL and how to retrieve each app's API key value
pnpm --filter @workflow-worlds/business-state-infra print-info
```

`print-info` only prints API key **ids** (`cdk-outputs.json` never contains key values). Retrieve
an actual value with:

```bash
aws apigateway get-api-key --api-key <id> --include-value --query value --output text
```

Deliver that value to each app out of band (e.g. your existing secrets manager) — v1 has no
self-service key provisioning.

To tear everything down (only works after an explicit destroy deploy — see "Naming and scope"
above):

```bash
pnpm --filter @workflow-worlds/business-state-infra run deploy -c apps=onboarding,billing -c removalPolicy=destroy
pnpm --filter @workflow-worlds/business-state-infra run destroy -c apps=onboarding,billing
```

## Adding a new app later

Redeploy with the new app name included in `-c apps=...` (existing apps' keys are untouched):

```bash
pnpm --filter @workflow-worlds/business-state-infra run deploy -c apps=onboarding,billing,referrals
```
