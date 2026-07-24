import { fileURLToPath } from 'node:url'
import { Stack, CfnOutput, RemovalPolicy, Duration } from 'aws-cdk-lib'
import type { StackProps } from 'aws-cdk-lib'
import { Table, AttributeType, BillingMode, ProjectionType } from 'aws-cdk-lib/aws-dynamodb'
import { Runtime } from 'aws-cdk-lib/aws-lambda'
import { NodejsFunction } from 'aws-cdk-lib/aws-lambda-nodejs'
import { RestApi, LambdaIntegration, ApiKey } from 'aws-cdk-lib/aws-apigateway'
import type { Construct } from 'constructs'

export interface BusinessStateStackProps extends StackProps {
  /**
   * Deployment stage. Defaults to 'prod' — unlike WorkflowAwsStack, this
   * deploys once as a single shared instance across every app in the org,
   * not once per project, so there's no `projectName` context param and
   * stage isn't expected to vary per deploy.
   */
  stage?: string
  /** App names to mint an API key for, e.g. ['onboarding', 'billing']. */
  apps: string[]
  /** Removal policy override. Defaults to RETAIN — this store may become the
   *  source of truth for business state across every app; unlike
   *  WorkflowAwsStack's prod-only rule, destruction always requires an
   *  explicit opt-in. */
  removalPolicy?: 'destroy' | 'retain'
}

export class BusinessStateStack extends Stack {
  constructor(scope: Construct, id: string, props: BusinessStateStackProps) {
    super(scope, id, props)

    const stage = props.stage ?? 'prod'
    const namePrefix = `business-state-${stage}`

    const tableRemovalPolicy = props.removalPolicy === 'destroy' ? RemovalPolicy.DESTROY : RemovalPolicy.RETAIN

    // -------------------------------------------------------------------
    // DynamoDB — mirrors packages/business-state/src/store.ts's key scheme:
    // PK = APP#<applicationId>#SUB#<subjectType>#<subjectId>, SK = PROC#<process>.
    // GSI1 is sparse (only written when a runId is supplied) and supports
    // "find state by run id" without a table scan.
    // -------------------------------------------------------------------
    const table = new Table(this, 'Table', {
      tableName: `${namePrefix}-table`,
      partitionKey: { name: 'PK', type: AttributeType.STRING },
      sortKey: { name: 'SK', type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      removalPolicy: tableRemovalPolicy,
    })

    table.addGlobalSecondaryIndex({
      indexName: 'GSI1',
      partitionKey: { name: 'GSI1PK', type: AttributeType.STRING },
      sortKey: { name: 'GSI1SK', type: AttributeType.STRING },
      projectionType: ProjectionType.ALL,
    })

    // -------------------------------------------------------------------
    // One API key per app, all on a single shared usage plan (v1 scope —
    // no per-app throttling). The apiKeyId -> applicationId map is baked
    // into the Lambda's environment below; `apiKey.keyId` is a deploy-time
    // CloudFormation token, so the map's *keys* are only resolved once
    // CloudFormation actually creates the ApiKey resources — this is the
    // standard way to thread a resource's generated id into another
    // resource's config without a second deploy.
    // -------------------------------------------------------------------
    const apiKeys: ApiKey[] = []
    const appKeyMapEntries: Array<[string, string]> = []

    for (const app of props.apps) {
      const apiKey = new ApiKey(this, `ApiKey-${app}`, {
        apiKeyName: `${namePrefix}-${app}`,
        description: `Business state API key for app "${app}"`,
      })
      apiKeys.push(apiKey)
      appKeyMapEntries.push([apiKey.keyId, app])
      new CfnOutput(this, `ApiKeyId-${app}`, { value: apiKey.keyId })
    }

    // -------------------------------------------------------------------
    // Lambda — bundled directly from TS source via esbuild (NodejsFunction),
    // not from a pre-built dist/, so infra deploys stay in sync with source
    // without a separate build-and-publish step for the handler package.
    // -------------------------------------------------------------------
    const handlerEntry = fileURLToPath(new URL('../../../src/handler.ts', import.meta.url))

    const fn = new NodejsFunction(this, 'Handler', {
      entry: handlerEntry,
      handler: 'handler',
      runtime: Runtime.NODEJS_20_X,
      timeout: Duration.seconds(10),
      environment: {
        BUSINESS_STATE_TABLE_NAME: table.tableName,
        BUSINESS_STATE_APP_KEY_MAP: JSON.stringify(Object.fromEntries(appKeyMapEntries)),
      },
    })
    table.grantReadWriteData(fn)

    // -------------------------------------------------------------------
    // REST API Gateway (v1) — required over HTTP API v2 for native API Key +
    // Usage Plan support. Routes match packages/business-state/src/handler.ts's
    // routeRequest():
    //   PUT/GET /state/{subjectType}/{subjectId}/{process}
    //   GET     /state/{subjectType}/{subjectId}
    //   GET     /runs/{runId}/state
    // -------------------------------------------------------------------
    const api = new RestApi(this, 'Api', {
      restApiName: `${namePrefix}-api`,
      deployOptions: { stageName: stage },
    })

    const integration = new LambdaIntegration(fn)

    const stateResource = api.root.addResource('state')
    const subjectTypeResource = stateResource.addResource('{subjectType}')
    const subjectIdResource = subjectTypeResource.addResource('{subjectId}')
    subjectIdResource.addMethod('GET', integration, { apiKeyRequired: true })

    const processResource = subjectIdResource.addResource('{process}')
    processResource.addMethod('GET', integration, { apiKeyRequired: true })
    processResource.addMethod('PUT', integration, { apiKeyRequired: true })

    const runsResource = api.root.addResource('runs')
    const runIdResource = runsResource.addResource('{runId}')
    const runStateResource = runIdResource.addResource('state')
    runStateResource.addMethod('GET', integration, { apiKeyRequired: true })

    const usagePlan = api.addUsagePlan('UsagePlan', {
      name: `${namePrefix}-usage-plan`,
      apiStages: [{ api, stage: api.deploymentStage }],
    })
    for (const apiKey of apiKeys) {
      usagePlan.addApiKey(apiKey)
    }

    // -------------------------------------------------------------------
    // Outputs — API key values are secrets, so only key ids are output here.
    // Retrieve the actual value with:
    //   aws apigateway get-api-key --api-key <id> --include-value
    // -------------------------------------------------------------------
    new CfnOutput(this, 'TableName', { value: table.tableName })
    new CfnOutput(this, 'ApiUrl', { value: api.url })
  }
}
