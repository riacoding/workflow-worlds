# @workflow-worlds/aws

## 0.1.5

### Patch Changes

- Fix `WORKFLOW_AWS_LOCAL`'s auto-started LocalStack container binding to a random host port instead of the one implied by `WORKFLOW_AWS_ENDPOINT`, and starting a duplicate container on every process restart instead of reusing the existing one.
