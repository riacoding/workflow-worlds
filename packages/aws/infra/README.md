# @workflow-worlds/aws-infra

AWS CDK (TypeScript) infrastructure for [`@workflow-worlds/aws`](../README.md). Deploys the same
resources that package's auto-provisioning code (`ensureTable`/`ensureQueue`/`ensureSchedulerGroup`
in `../src/aws.ts`) creates at first run — as real, versioned CloudFormation — plus the IAM role
the EventBridge Scheduler needs to deliver to SQS for delays beyond SQS's 12-hour ceiling (today a
manual step, see the parent README's "Provisioning for real AWS" section).

**Deploying is entirely up to you.** This project doesn't deploy anything on its own — you run
`cdk bootstrap`/`deploy`/`destroy` yourself, whenever you're ready.

## What gets deployed

| Resource | Mirrors |
|---|---|
| DynamoDB table (`PK`/`SK`, GSI1, GSI2, TTL on `ttl`) | `ensureTable()` |
| SQS standard queue (60s visibility, 14-day retention) | `ensureQueue()` |
| EventBridge Scheduler group | `ensureSchedulerGroup()` |
| IAM role for the Scheduler (`scheduler.amazonaws.com` trust, `sqs:SendMessage` on the queue) | not auto-provisioned today — manual step, now automated |

## Naming

This stack is meant to be deployed once per **project** using the Workflow DevKit (not as a
shared, org-wide backend), and once per **stage** (dev/staging/prod) within that project. Every
resource — and the CloudFormation stack itself — is namespaced `${projectName}-${stage}-...`:

- `projectName` — **required**, no default. `cdk synth`/`deploy` fails with a clear error if
  it's not provided, rather than silently colliding with another project's stack on a generic
  default name.
- `stage` — optional, defaults to `dev`.
- `removalPolicy` — optional (`destroy` | `retain`). Defaults to `retain` for the DynamoDB table
  when `stage=prod`, `destroy` otherwise. The SQS queue and IAM role always use CDK's default
  (`destroy`).

Pass these via `-c`, e.g. `-c projectName=myapp -c stage=dev`, on every `cdk`/`pnpm` command
below — or persist them once in a `cdk.context.json` file in this directory (gitignored by
default; commit it yourself if you want a fixed default checked into your repo).

## Prerequisites

- An AWS account and credentials configured (e.g. `AWS_PROFILE`, or `aws configure`).
- `cdk bootstrap` run once per account/region (first-time CDK setup only):
  ```bash
  pnpm exec cdk bootstrap
  ```

## Deploy workflow

```bash
# 1. Install + build (from the monorepo root, or within this directory)
pnpm install
pnpm --filter @workflow-worlds/aws-infra build

# 2. (Optional) preview the changes
pnpm --filter @workflow-worlds/aws-infra diff -- -c projectName=myapp -c stage=dev

# 3. Deploy — writes outputs to cdk-outputs.json
pnpm --filter @workflow-worlds/aws-infra deploy -- -c projectName=myapp -c stage=dev

# 4. Turn the deployed resource identifiers into WORKFLOW_* env vars
eval "$(pnpm --filter @workflow-worlds/aws-infra print-env)"

# Your shell now has WORKFLOW_AWS_REGION, WORKFLOW_DYNAMODB_TABLE_NAME,
# WORKFLOW_SQS_QUEUE_URL, WORKFLOW_SQS_QUEUE_ARN, WORKFLOW_SCHEDULER_GROUP_NAME,
# WORKFLOW_SCHEDULER_ROLE_ARN, WORKFLOW_AWS_LOCAL=false, WORKFLOW_AWS_AUTO_PROVISION=false set.
# Run your app (WORKFLOW_TARGET_WORLD=@workflow-worlds/aws) in the same shell/process,
# or copy these into whatever secret/env store your deployment target uses.
```

To tear everything down:

```bash
pnpm --filter @workflow-worlds/aws-infra destroy -- -c projectName=myapp -c stage=dev
```

## `recreate-table` (dev convenience)

Wipes all workflow data by deleting and recreating the DynamoDB table, instead of hand-deleting
every item. It inspects the table first (status, item count, key schema, GSIs, TTL), then requires
typing the table name back to confirm before deleting.

```bash
# Reads table name/region from cdk-outputs.json if omitted
pnpm --filter @workflow-worlds/aws-infra recreate-table

# Or pass explicitly
pnpm --filter @workflow-worlds/aws-infra recreate-table -- myapp-dev-workflow

# Inspect only, no delete
pnpm --filter @workflow-worlds/aws-infra recreate-table -- myapp-dev-workflow --inspect-only

# Skip the interactive confirmation (e.g. scripting)
pnpm --filter @workflow-worlds/aws-infra recreate-table -- myapp-dev-workflow --yes
```

The recreated table's schema is hardcoded in `scripts/recreate-table.sh` to exactly mirror the
`Table` construct in `src/lib/workflow-aws-stack.ts` (same name, `PK`/`SK`, `GSI1`, `GSI2`,
`PAY_PER_REQUEST`, TTL on `ttl`) — this table is normally CDK-managed, so recreating it out of band
only stays safe (i.e., a later `cdk diff` shows no changes) as long as those two definitions match.
If you ever change the table's shape in the CDK stack, update the script to match. The script
refuses to run against a table name containing `prod`.

## `print-env`

Reads `cdk-outputs.json` (produced by `pnpm deploy`, which always passes `--outputs-file
cdk-outputs.json`) and prints `export WORKFLOW_...=...` lines to stdout — nothing else goes to
stdout, so it's always safe to `eval`. Errors and diagnostics go to stderr. Pass an explicit path
as the first argument if you've moved/renamed the outputs file: `pnpm print-env path/to/outputs.json`.
