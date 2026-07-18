# AWS World vNext – Developer Experience

## Goal

The AWS World is functionally complete:

* Passes the official Workflow conformance suite.
* Runs against LocalStack.
* Runs against real AWS.
* Works with `workflow inspect` and `workflow web`.

The next phase is **not new functionality**. It is reducing the friction required to use the package.

The objective is to make the AWS World feel as seamless to use as the built-in Vercel experience.

---

# Current Experience

Today a developer typically performs the following steps:

1. Build the AWS World package.
2. Package and install it into the application.
3. Deploy infrastructure via CDK.
4. Configure numerous environment variables.
5. Start the application.
6. Remember to pass `--backend=@workflow-worlds/aws` to CLI commands.
7. Remember to export AWS/LocalStack configuration in every terminal.

The runtime works correctly, but the workflow is more manual than it should be.

---

# Desired Experience

A developer should be able to install and deploy with only a few commands.

```bash
pnpm add @workflow-worlds/aws

npx workflow-aws deploy --stage dev

pnpm dev
```

Inspection should feel equally natural.

```bash
npx workflow-aws inspect runs

npx workflow-aws web <runId>
```

The user should never have to remember backend names, queue URLs, or infrastructure resource names.

---

# Proposed CLI

The package should expose a CLI.

```text
workflow-aws
```

Example commands:

```bash
workflow-aws deploy
workflow-aws destroy
workflow-aws inspect
workflow-aws web
workflow-aws doctor
workflow-aws env
workflow-aws outputs
```

---

# Deployment

The deploy command should become the primary onboarding experience.

```bash
workflow-aws deploy \
    --stage dev \
    --region us-west-2
```

Responsibilities:

* CDK synth
* CDK deploy
* Create/update infrastructure
* Display deployment outputs
* Optionally generate local configuration

---

# Local Configuration

After deployment, create a project-local configuration file.

Example:

```text
.workflow/aws.json
```

Example contents:

```json
{
  "backend": "@workflow-worlds/aws",
  "region": "us-west-2",
  "tableName": "workflow-dev",
  "queueUrl": "...",
  "schedulerGroup": "workflow-dev"
}
```

This file should contain deployment metadata only.

AWS credentials should continue to use the standard AWS credential chain:

* AWS Profile
* IAM Identity Center (SSO)
* Environment variables
* EC2/Lambda roles

---

# Automatic CLI Configuration

Current:

```bash
export WORKFLOW_AWS_ENDPOINT=...
export WORKFLOW_AWS_REGION=...
...

npx workflow inspect runs --backend=@workflow-worlds/aws
```

Desired:

```bash
workflow-aws inspect runs
```

The wrapper CLI should:

* load the local configuration
* inject required environment variables
* invoke the standard Workflow CLI

The same applies to:

```bash
workflow-aws web <runId>
```

No backend parameter should be required.

---

# LocalStack Support

Current support already exists through:

```text
WORKFLOW_AWS_LOCAL=true
```

The CLI should make this transparent.

Example:

```bash
workflow-aws deploy --local
```

or

```bash
workflow-aws up
```

Responsibilities:

* Start LocalStack (optional)
* Configure endpoint
* Create infrastructure
* Write local configuration

---

# Doctor Command

A diagnostic command should verify the local environment.

Checks:

* AWS credentials
* LocalStack availability
* DynamoDB table exists
* GSIs exist
* TTL enabled
* Queue exists
* Scheduler configuration
* Package import
* Workflow CLI compatibility

Example:

```bash
workflow-aws doctor
```

---

# Outputs Command

Display deployment outputs in multiple formats.

Examples:

```bash
workflow-aws outputs

workflow-aws outputs --json

workflow-aws outputs --shell

workflow-aws outputs --write .env.local
```

---

# Design Principles

The AWS World should remain focused on runtime execution.

Responsibilities:

* Storage
* Queue
* Streamer

Infrastructure deployment and developer tooling should be layered on top rather than embedded into the runtime.

The runtime should never perform infrastructure deployment.

---

# Future Opportunities

Potential enhancements after the developer experience is complete:

* Amplify environment integration
* CDK construct library
* CloudFormation outputs
* CloudWatch dashboards
* X-Ray tracing
* GitHub Actions deployment templates
* VS Code extension integration

These are explicitly out of scope for the next iteration.

---

# Success Criteria

A new developer should be able to:

1. Install the package.
2. Deploy AWS infrastructure.
3. Start a Workflow application.
4. Inspect workflow runs.
5. Open the Workflow web UI.

...without needing to manually discover backend names, resource names, queue URLs, or repeated environment variables.

The goal is to make the AWS World feel like a first-class Workflow deployment target rather than an advanced manual integration.
