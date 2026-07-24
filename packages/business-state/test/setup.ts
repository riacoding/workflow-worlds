/**
 * Shared test setup for the business-state store.
 *
 * Spins up a LocalStack container (DynamoDB only) and creates the table with
 * the same PK/SK + GSI1 schema as infra/src/lib/business-state-stack.ts.
 *
 * If WORKFLOW_AWS_ENDPOINT is already set (e.g. a CI-provided LocalStack),
 * the container is skipped.
 */

import {
  CreateTableCommand,
  DescribeTableCommand,
  DynamoDBClient,
  ResourceNotFoundException,
} from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { LocalstackContainer } from '@testcontainers/localstack';
import { afterAll, beforeAll } from 'vitest';
import { createBusinessStateStore, type BusinessStateStore } from '../dist/store.js';

type StartedLocalstack = Awaited<ReturnType<LocalstackContainer['start']>>;

let container: StartedLocalstack | null = null;
let startedLocalContainer = false;

const TABLE_NAME = 'business-state-test';

beforeAll(async () => {
  if (process.env.WORKFLOW_AWS_ENDPOINT) {
    console.log('Using existing AWS endpoint:', process.env.WORKFLOW_AWS_ENDPOINT);
    return;
  }

  console.log('Starting LocalStack container...');
  container = await new LocalstackContainer('localstack/localstack:3').start();
  startedLocalContainer = true;

  const endpoint = container.getConnectionUri();
  console.log('LocalStack started:', endpoint);

  process.env.WORKFLOW_AWS_ENDPOINT = endpoint;
  process.env.WORKFLOW_AWS_REGION = 'us-west-2';
  process.env.WORKFLOW_AWS_ACCESS_KEY_ID = 'test';
  process.env.WORKFLOW_AWS_SECRET_ACCESS_KEY = 'test';

  await ensureTable();
}, 180_000);

afterAll(async () => {
  if (container) {
    console.log('Stopping LocalStack container...');
    await container.stop();
    container = null;
  }
  if (startedLocalContainer) {
    delete process.env.WORKFLOW_AWS_ENDPOINT;
    startedLocalContainer = false;
  }
});

function rawClient(): DynamoDBClient {
  return new DynamoDBClient({
    region: process.env.WORKFLOW_AWS_REGION ?? 'us-west-2',
    endpoint: process.env.WORKFLOW_AWS_ENDPOINT,
    credentials: {
      accessKeyId: process.env.WORKFLOW_AWS_ACCESS_KEY_ID ?? 'test',
      secretAccessKey: process.env.WORKFLOW_AWS_SECRET_ACCESS_KEY ?? 'test',
    },
  });
}

async function waitForTableActive(client: DynamoDBClient, timeoutMs = 60_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await client.send(new DescribeTableCommand({ TableName: TABLE_NAME }));
      if (res.Table?.TableStatus === 'ACTIVE') return;
    } catch (err) {
      if (!(err instanceof ResourceNotFoundException)) throw err;
    }
    await new Promise((r) => setTimeout(r, 250));
  }
  throw new Error(`Timed out waiting for DynamoDB table ${TABLE_NAME}`);
}

async function ensureTable(): Promise<void> {
  const client = rawClient();
  try {
    const res = await client.send(new DescribeTableCommand({ TableName: TABLE_NAME }));
    if (res.Table?.TableStatus === 'ACTIVE') return;
    await waitForTableActive(client);
    return;
  } catch (err) {
    if (!(err instanceof ResourceNotFoundException)) throw err;
  }

  await client.send(
    new CreateTableCommand({
      TableName: TABLE_NAME,
      BillingMode: 'PAY_PER_REQUEST',
      AttributeDefinitions: [
        { AttributeName: 'PK', AttributeType: 'S' },
        { AttributeName: 'SK', AttributeType: 'S' },
        { AttributeName: 'GSI1PK', AttributeType: 'S' },
        { AttributeName: 'GSI1SK', AttributeType: 'S' },
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
      ],
    })
  );

  await waitForTableActive(client);
}

let storePromise: Promise<BusinessStateStore> | null = null;

export async function getTestStore(): Promise<BusinessStateStore> {
  if (!storePromise) {
    storePromise = (async () => {
      const rawDdb = rawClient();
      const ddb = DynamoDBDocumentClient.from(rawDdb, {
        marshallOptions: { removeUndefinedValues: true },
      });
      return createBusinessStateStore({ ddb, tableName: TABLE_NAME });
    })();
  }
  return storePromise;
}
