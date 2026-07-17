#!/usr/bin/env node
import { App } from 'aws-cdk-lib'
import { WorkflowAwsStack } from '../lib/workflow-aws-stack.js'

const app = new App()

const projectName = app.node.tryGetContext('projectName') as string | undefined
if (!projectName) {
  throw new Error(
    "Missing required CDK context 'projectName'. Pass it via `-c projectName=<yourproject>` " +
      '(e.g. `pnpm deploy -- -c projectName=myapp -c stage=dev`), or persist it in cdk.context.json. ' +
      'This namespaces every resource so multiple projects can share an AWS account/region without name collisions.',
  )
}

const stage = (app.node.tryGetContext('stage') as string | undefined) ?? 'dev'

const removalPolicyContext = app.node.tryGetContext('removalPolicy') as string | undefined
if (removalPolicyContext !== undefined && removalPolicyContext !== 'destroy' && removalPolicyContext !== 'retain') {
  throw new Error(`Invalid -c removalPolicy=${removalPolicyContext}; expected 'destroy' or 'retain'.`)
}

new WorkflowAwsStack(app, `${projectName}-${stage}-WorkflowAwsStack`, {
  projectName,
  stage,
  removalPolicy: removalPolicyContext as 'destroy' | 'retain' | undefined,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
})
