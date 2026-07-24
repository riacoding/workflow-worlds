#!/usr/bin/env node
import { App } from 'aws-cdk-lib'
import { BusinessStateStack } from '../lib/business-state-stack.js'

const app = new App()

const appsContext = app.node.tryGetContext('apps') as string | undefined
if (!appsContext) {
  throw new Error(
    "Missing required CDK context 'apps'. Pass the app names that should get an API key, " +
      'e.g. `pnpm deploy -- -c apps=onboarding,billing`.',
  )
}
const apps = appsContext
  .split(',')
  .map((a) => a.trim())
  .filter(Boolean)
if (apps.length === 0) {
  throw new Error("-c apps=... must contain at least one app name.")
}

const stage = (app.node.tryGetContext('stage') as string | undefined) ?? 'prod'

const removalPolicyContext = app.node.tryGetContext('removalPolicy') as string | undefined
if (removalPolicyContext !== undefined && removalPolicyContext !== 'destroy' && removalPolicyContext !== 'retain') {
  throw new Error(`Invalid -c removalPolicy=${removalPolicyContext}; expected 'destroy' or 'retain'.`)
}

new BusinessStateStack(app, `business-state-${stage}-BusinessStateStack`, {
  stage,
  apps,
  removalPolicy: removalPolicyContext as 'destroy' | 'retain' | undefined,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
})
