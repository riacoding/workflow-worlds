/**
 * Shared test setup for the AWS World.
 *
 * Spins up a LocalStack container (DynamoDB + SQS, and — where supported —
 * EventBridge Scheduler) so the world can run against emulated AWS services.
 *
 * If WORKFLOW_AWS_ENDPOINT is already set (e.g. a CI-provided LocalStack or a
 * real AWS test account), the container is skipped.
 */

import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { LocalstackContainer } from '@testcontainers/localstack';
import { afterAll, beforeAll } from 'vitest';

const __dirname = dirname(fileURLToPath(import.meta.url));

type StartedLocalstack = Awaited<
  ReturnType<LocalstackContainer['start']>
>;

let container: StartedLocalstack | null = null;
let startedLocalContainer = false;

const TABLE_NAME = 'workflow-test';
const QUEUE_NAME = 'workflow-test-queue';

beforeAll(async () => {
  if (process.env.WORKFLOW_AWS_ENDPOINT) {
    // eslint-disable-next-line no-console
    console.log('Using existing AWS endpoint:', process.env.WORKFLOW_AWS_ENDPOINT);
    return;
  }

  // eslint-disable-next-line no-console
  console.log('Starting LocalStack container...');
  container = await new LocalstackContainer('localstack/localstack:3').start();
  startedLocalContainer = true;

  const endpoint = container.getConnectionUri();
  // eslint-disable-next-line no-console
  console.log('LocalStack started:', endpoint);

  process.env.WORKFLOW_AWS_ENDPOINT = endpoint;
  process.env.WORKFLOW_AWS_REGION = 'us-east-1';
  process.env.WORKFLOW_AWS_ACCESS_KEY_ID = 'test';
  process.env.WORKFLOW_AWS_SECRET_ACCESS_KEY = 'test';
  process.env.WORKFLOW_DYNAMODB_TABLE_NAME = TABLE_NAME;
  process.env.WORKFLOW_SQS_QUEUE_NAME = QUEUE_NAME;
}, 180_000);

afterAll(async () => {
  if (container) {
    // eslint-disable-next-line no-console
    console.log('Stopping LocalStack container...');
    await container.stop();
    container = null;
  }
  if (startedLocalContainer) {
    delete process.env.WORKFLOW_AWS_ENDPOINT;
    startedLocalContainer = false;
  }
});

export const worldPath = join(__dirname, '..', 'dist', 'index.js');

// -----------------------------------------------------------------------------
// Lazily-provisioned clients for the component-level contract tests.
// -----------------------------------------------------------------------------

let clientsPromise: Promise<{ ddb: unknown; tableName: string }> | null = null;

async function getClients(): Promise<{ ddb: unknown; tableName: string }> {
  if (!clientsPromise) {
    clientsPromise = (async () => {
      const awsMod = await import(join(__dirname, '..', 'dist', 'aws.js'));
      const resolved = awsMod.resolveAwsConfig({});
      const clients = awsMod.createAwsClients(resolved);
      await awsMod.ensureTable(clients);
      await awsMod.ensureQueue(clients);
      return { ddb: clients.ddb, tableName: resolved.tableName };
    })();
  }
  return clientsPromise;
}

export async function createStorage() {
  const { ddb, tableName } = await getClients();
  const mod = await import(join(__dirname, '..', 'dist', 'storage.js'));
  return { storage: mod.createStorage({ ddb, tableName }) };
}

export async function createStreamer() {
  const { ddb, tableName } = await getClients();
  const mod = await import(join(__dirname, '..', 'dist', 'streamer.js'));
  // Disable the DynamoDB tail poll in tests — everything is single-process, so
  // the in-process emitter delivers realtime chunks and the poll only adds load.
  return {
    streamer: mod.createStreamer({ ddb, tableName, enableTailPoll: false }),
  };
}
