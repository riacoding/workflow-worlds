/**
 * Shared AWS context: client construction, configuration resolution and
 * one-time resource provisioning for the AWS World.
 *
 * All configuration follows the priority: config value > env var > default.
 * Connection-style settings use the `WORKFLOW_` prefix and `URI`/`ENDPOINT`
 * naming (never `URL`) per the repository conventions.
 */

import {
  CreateTableCommand,
  DescribeTableCommand,
  DynamoDBClient,
  ResourceNotFoundException,
  UpdateTimeToLiveCommand,
} from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import {
  CreateQueueCommand,
  GetQueueUrlCommand,
  QueueDoesNotExist,
  SQSClient,
} from '@aws-sdk/client-sqs';
import {
  CreateScheduleGroupCommand,
  SchedulerClient,
} from '@aws-sdk/client-scheduler';
import { debug } from './utils.js';

// =============================================================================
// Configuration
// =============================================================================

export interface AwsConfig {
  /** AWS region. Env: WORKFLOW_AWS_REGION. Default: us-east-1. */
  region?: string;
  /**
   * Custom endpoint for all AWS services (used for LocalStack / emulation).
   * Env: WORKFLOW_AWS_ENDPOINT.
   */
  endpoint?: string;
  /** Static access key id (mostly for emulation). Env: WORKFLOW_AWS_ACCESS_KEY_ID. */
  accessKeyId?: string;
  /** Static secret access key (mostly for emulation). Env: WORKFLOW_AWS_SECRET_ACCESS_KEY. */
  secretAccessKey?: string;

  /** DynamoDB single-table name. Env: WORKFLOW_DYNAMODB_TABLE_NAME. Default: workflow. */
  tableName?: string;
  /**
   * When true (default), the world creates the DynamoDB table and SQS queue if
   * they do not exist. Set false in production where infra is managed by IaC.
   * Env: WORKFLOW_AWS_AUTO_PROVISION.
   */
  autoProvision?: boolean;

  /** Full SQS queue URL. Env: WORKFLOW_SQS_QUEUE_URL. */
  queueUrl?: string;
  /** SQS queue name (used to resolve/create the URL). Env: WORKFLOW_SQS_QUEUE_NAME. Default: workflow-queue. */
  queueName?: string;

  /** EventBridge Scheduler group name. Env: WORKFLOW_SCHEDULER_GROUP_NAME. Default: workflow. */
  schedulerGroupName?: string;
  /** IAM role ARN the scheduler assumes to deliver to SQS. Env: WORKFLOW_SCHEDULER_ROLE_ARN. */
  schedulerRoleArn?: string;
  /** ARN of the SQS queue (target for scheduled delivery). Env: WORKFLOW_SQS_QUEUE_ARN. */
  queueArn?: string;

  /**
   * AppSync Events API HTTP endpoint for publishing stream chunks, e.g.
   * https://<api-id>.appsync-api.<region>.amazonaws.com/event
   * Env: WORKFLOW_APPSYNC_EVENTS_ENDPOINT.
   */
  appsyncEventsEndpoint?: string;
  /** AppSync Events API key (x-api-key auth). Env: WORKFLOW_APPSYNC_API_KEY. */
  appsyncApiKey?: string;
}

export interface ResolvedAwsConfig {
  region: string;
  endpoint?: string;
  credentials?: { accessKeyId: string; secretAccessKey: string };
  tableName: string;
  autoProvision: boolean;
  queueUrl?: string;
  queueName: string;
  schedulerGroupName: string;
  schedulerRoleArn?: string;
  queueArn?: string;
  appsyncEventsEndpoint?: string;
  appsyncApiKey?: string;
}

function envBool(value: string | undefined): boolean | undefined {
  if (value === undefined) return undefined;
  return value === 'true' || value === '1';
}

export function resolveAwsConfig(config: AwsConfig = {}): ResolvedAwsConfig {
  const region =
    config.region ??
    process.env.WORKFLOW_AWS_REGION ??
    process.env.AWS_REGION ??
    'us-east-1';

  const endpoint = config.endpoint ?? process.env.WORKFLOW_AWS_ENDPOINT;

  const accessKeyId =
    config.accessKeyId ?? process.env.WORKFLOW_AWS_ACCESS_KEY_ID;
  const secretAccessKey =
    config.secretAccessKey ?? process.env.WORKFLOW_AWS_SECRET_ACCESS_KEY;

  const credentials =
    accessKeyId && secretAccessKey
      ? { accessKeyId, secretAccessKey }
      : undefined;

  return {
    region,
    endpoint,
    credentials,
    tableName:
      config.tableName ??
      process.env.WORKFLOW_DYNAMODB_TABLE_NAME ??
      'workflow',
    autoProvision:
      config.autoProvision ??
      envBool(process.env.WORKFLOW_AWS_AUTO_PROVISION) ??
      true,
    queueUrl: config.queueUrl ?? process.env.WORKFLOW_SQS_QUEUE_URL,
    queueName:
      config.queueName ??
      process.env.WORKFLOW_SQS_QUEUE_NAME ??
      'workflow-queue',
    schedulerGroupName:
      config.schedulerGroupName ??
      process.env.WORKFLOW_SCHEDULER_GROUP_NAME ??
      'workflow',
    schedulerRoleArn:
      config.schedulerRoleArn ?? process.env.WORKFLOW_SCHEDULER_ROLE_ARN,
    queueArn: config.queueArn ?? process.env.WORKFLOW_SQS_QUEUE_ARN,
    appsyncEventsEndpoint:
      config.appsyncEventsEndpoint ??
      process.env.WORKFLOW_APPSYNC_EVENTS_ENDPOINT,
    appsyncApiKey:
      config.appsyncApiKey ?? process.env.WORKFLOW_APPSYNC_API_KEY,
  };
}

// =============================================================================
// Clients
// =============================================================================

export interface AwsClients {
  config: ResolvedAwsConfig;
  ddb: DynamoDBDocumentClient;
  rawDdb: DynamoDBClient;
  sqs: SQSClient;
  scheduler: SchedulerClient;
}

export function createAwsClients(config: ResolvedAwsConfig): AwsClients {
  const shared = {
    region: config.region,
    ...(config.endpoint ? { endpoint: config.endpoint } : {}),
    ...(config.credentials ? { credentials: config.credentials } : {}),
  };

  const rawDdb = new DynamoDBClient(shared);
  const ddb = DynamoDBDocumentClient.from(rawDdb, {
    marshallOptions: {
      removeUndefinedValues: true,
      convertClassInstanceToMap: false,
    },
  });
  const sqs = new SQSClient(shared);
  const scheduler = new SchedulerClient(shared);

  return { config, ddb, rawDdb, sqs, scheduler };
}

// =============================================================================
// Provisioning (auto-create table / queue for local dev & emulation)
// =============================================================================

async function waitForTableActive(
  rawDdb: DynamoDBClient,
  tableName: string,
  timeoutMs = 60_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await rawDdb.send(
        new DescribeTableCommand({ TableName: tableName })
      );
      if (res.Table?.TableStatus === 'ACTIVE') return;
    } catch (err) {
      if (!(err instanceof ResourceNotFoundException)) throw err;
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`Timed out waiting for DynamoDB table ${tableName}`);
}

export async function ensureTable(clients: AwsClients): Promise<void> {
  const { rawDdb, config } = clients;
  const tableName = config.tableName;

  try {
    const res = await rawDdb.send(
      new DescribeTableCommand({ TableName: tableName })
    );
    if (res.Table?.TableStatus === 'ACTIVE') return;
    await waitForTableActive(rawDdb, tableName);
    return;
  } catch (err) {
    if (!(err instanceof ResourceNotFoundException)) throw err;
  }

  debug('Creating DynamoDB table:', tableName);
  try {
    await rawDdb.send(
      new CreateTableCommand({
        TableName: tableName,
        BillingMode: 'PAY_PER_REQUEST',
        AttributeDefinitions: [
          { AttributeName: 'PK', AttributeType: 'S' },
          { AttributeName: 'SK', AttributeType: 'S' },
          { AttributeName: 'GSI1PK', AttributeType: 'S' },
          { AttributeName: 'GSI1SK', AttributeType: 'S' },
          { AttributeName: 'GSI2PK', AttributeType: 'S' },
          { AttributeName: 'GSI2SK', AttributeType: 'S' },
        ],
        KeySchema: [
          { AttributeName: 'PK', KeyType: 'HASH' },
          { AttributeName: 'SK', KeyType: 'RANGE' },
        ],
        GlobalSecondaryIndexes: [
          {
            IndexName: 'GSI1',
            KeySchema: [
              { AttributeName: 'GSI1PK', KeyType: 'HASH' },
              { AttributeName: 'GSI1SK', KeyType: 'RANGE' },
            ],
            Projection: { ProjectionType: 'ALL' },
          },
          {
            IndexName: 'GSI2',
            KeySchema: [
              { AttributeName: 'GSI2PK', KeyType: 'HASH' },
              { AttributeName: 'GSI2SK', KeyType: 'RANGE' },
            ],
            Projection: { ProjectionType: 'ALL' },
          },
        ],
      })
    );
  } catch (err) {
    // Another process may have created it concurrently.
    if (
      !(err instanceof Error) ||
      !/already exists|ResourceInUse/i.test(err.name + err.message)
    ) {
      throw err;
    }
  }

  await waitForTableActive(rawDdb, tableName);

  // Enable TTL on the `ttl` attribute (used by queue idempotency records).
  try {
    await rawDdb.send(
      new UpdateTimeToLiveCommand({
        TableName: tableName,
        TimeToLiveSpecification: { Enabled: true, AttributeName: 'ttl' },
      })
    );
  } catch (err) {
    debug('Could not enable TTL (non-fatal):', String(err));
  }
}

export async function ensureQueue(clients: AwsClients): Promise<string> {
  const { sqs, config } = clients;
  if (config.queueUrl) return config.queueUrl;

  try {
    const res = await sqs.send(
      new GetQueueUrlCommand({ QueueName: config.queueName })
    );
    if (res.QueueUrl) {
      config.queueUrl = res.QueueUrl;
      return res.QueueUrl;
    }
  } catch (err) {
    if (!(err instanceof QueueDoesNotExist)) {
      // Some emulators throw a generic error for a missing queue.
      debug('GetQueueUrl failed, attempting create:', String(err));
    }
  }

  debug('Creating SQS queue:', config.queueName);
  const created = await sqs.send(
    new CreateQueueCommand({
      QueueName: config.queueName,
      Attributes: {
        // Long enough for a step HTTP callback; redelivery happens after this.
        VisibilityTimeout: '60',
        MessageRetentionPeriod: '1209600',
      },
    })
  );
  config.queueUrl = created.QueueUrl!;
  return config.queueUrl;
}

export async function ensureSchedulerGroup(clients: AwsClients): Promise<void> {
  const { scheduler, config } = clients;
  try {
    await scheduler.send(
      new CreateScheduleGroupCommand({ Name: config.schedulerGroupName })
    );
    debug('Created scheduler group:', config.schedulerGroupName);
  } catch (err) {
    // ConflictException = already exists; anything else is non-fatal because
    // the scheduler is only used for long (>15m) delays which the test suite
    // does not exercise.
    debug('ensureSchedulerGroup non-fatal:', String(err));
  }
}
