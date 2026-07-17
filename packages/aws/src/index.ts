/**
 * AWS World Implementation
 *
 * A World backed entirely by AWS managed services:
 *   - Storage:    DynamoDB (single-table design)
 *   - Queue:      SQS (+ DynamoDB TTL idempotency records)
 *   - Scheduling: EventBridge Scheduler (delayed / long-running retries)
 *   - Streamer:   DynamoDB persistence + AppSync Events API pub/sub
 *
 * The default export is the `createWorld` factory function itself (never a call
 * to it) so the runtime can import and invoke it via WORKFLOW_TARGET_WORLD.
 */

import type { World } from '@workflow/world';
import { GetQueueAttributesCommand } from '@aws-sdk/client-sqs';
import {
  createAwsClients,
  ensureQueue,
  ensureSchedulerGroup,
  ensureTable,
  resolveAwsConfig,
  type AwsClients,
  type AwsConfig,
} from './aws.js';
import { createStorage } from './storage.js';
import { createQueue } from './queue.js';
import { createStreamer } from './streamer.js';
import { debug } from './utils.js';

export interface AwsWorldConfig extends AwsConfig {
  /** Base URL for HTTP callbacks. Env: WORKFLOW_SERVICE_URL. */
  baseUrl?: string;
  /** Max concurrent message processing. Env: WORKFLOW_CONCURRENCY. */
  concurrency?: number;
}

interface Initialized {
  clients: AwsClients;
  // Typed as `any` so the delegating World wrappers below don't have to
  // re-satisfy every resolveData overload of the Storage/Queue/Streamer methods.
  storage: any;
  queue: any;
  startQueue: () => Promise<void>;
  closeQueue: () => Promise<void>;
  streamer: any;
}

export function createWorld(config: AwsWorldConfig = {}): World {
  const resolved = resolveAwsConfig(config);

  debug('Creating AWS world:', {
    region: resolved.region,
    endpoint: resolved.endpoint ?? '(default)',
    tableName: resolved.tableName,
    queueName: resolved.queueName,
    autoProvision: resolved.autoProvision,
  });

  let initPromise: Promise<Initialized> | null = null;

  function ensureInitialized(): Promise<Initialized> {
    if (initPromise) return initPromise;

    initPromise = (async () => {
      const clients = createAwsClients(resolved);

      if (resolved.autoProvision) {
        await Promise.all([ensureTable(clients), ensureQueue(clients)]);
        await ensureSchedulerGroup(clients);
      } else if (!resolved.queueUrl) {
        await ensureQueue(clients);
      }

      const queueUrl = resolved.queueUrl!;

      // Resolve the queue ARN (needed only when EventBridge Scheduler is used
      // for long delays). Best-effort — never block startup on it.
      let queueArn = resolved.queueArn;
      if (!queueArn) {
        try {
          const attrs = await clients.sqs.send(
            new GetQueueAttributesCommand({
              QueueUrl: queueUrl,
              AttributeNames: ['QueueArn'],
            })
          );
          queueArn = attrs.Attributes?.QueueArn;
        } catch (err) {
          debug('Could not resolve queue ARN (non-fatal):', String(err));
        }
      }

      const storage = createStorage({
        ddb: clients.ddb,
        tableName: resolved.tableName,
      });

      const { queue, start, close } = createQueue({
        ddb: clients.ddb,
        sqs: clients.sqs,
        scheduler: clients.scheduler,
        tableName: resolved.tableName,
        queueUrl,
        baseUrl: config.baseUrl,
        concurrency: config.concurrency,
        schedulerGroupName: resolved.schedulerGroupName,
        schedulerRoleArn: resolved.schedulerRoleArn,
        queueArn,
      });

      const streamer = createStreamer({
        ddb: clients.ddb,
        tableName: resolved.tableName,
        appsyncEventsEndpoint: resolved.appsyncEventsEndpoint,
        appsyncApiKey: resolved.appsyncApiKey,
      });

      debug('AWS world initialization complete');

      return {
        clients,
        storage,
        queue,
        startQueue: start,
        closeQueue: close,
        streamer,
      };
    })();

    return initPromise;
  }

  return {
    // =========================================================================
    // STORAGE
    // =========================================================================
    runs: {
      async get(id, params) {
        const { storage } = await ensureInitialized();
        return storage.runs.get(id, params);
      },
      async list(params) {
        const { storage } = await ensureInitialized();
        return storage.runs.list(params);
      },
    },
    steps: {
      async get(runId, stepId, params) {
        const { storage } = await ensureInitialized();
        return storage.steps.get(runId, stepId, params);
      },
      async list(params) {
        const { storage } = await ensureInitialized();
        return storage.steps.list(params);
      },
    },
    events: {
      async create(runId, data, params) {
        const { storage } = await ensureInitialized();
        return storage.events.create(runId as never, data as never, params);
      },
      async list(params) {
        const { storage } = await ensureInitialized();
        return storage.events.list(params);
      },
      async listByCorrelationId(params) {
        const { storage } = await ensureInitialized();
        return storage.events.listByCorrelationId(params);
      },
    },
    hooks: {
      async get(hookId, params) {
        const { storage } = await ensureInitialized();
        return storage.hooks.get(hookId, params);
      },
      async getByToken(token, params) {
        const { storage } = await ensureInitialized();
        return storage.hooks.getByToken(token, params);
      },
      async list(params) {
        const { storage } = await ensureInitialized();
        return storage.hooks.list(params);
      },
    },

    // =========================================================================
    // QUEUE
    // =========================================================================
    async getDeploymentId() {
      const { queue } = await ensureInitialized();
      return queue.getDeploymentId();
    },
    async queue(queueName, message, opts) {
      const { queue } = await ensureInitialized();
      return queue.queue(queueName, message, opts);
    },
    createQueueHandler(prefix, handler) {
      return async (req: Request): Promise<Response> => {
        const { queue } = await ensureInitialized();
        return queue.createQueueHandler(prefix, handler)(req);
      };
    },

    // =========================================================================
    // STREAMER
    // =========================================================================
    async writeToStream(name, runId, chunk) {
      const { streamer } = await ensureInitialized();
      return streamer.writeToStream(name, runId, chunk);
    },
    async closeStream(name, runId) {
      const { streamer } = await ensureInitialized();
      return streamer.closeStream(name, runId);
    },
    async readFromStream(name, startIndex) {
      const { streamer } = await ensureInitialized();
      return streamer.readFromStream(name, startIndex);
    },
    async listStreamsByRunId(runId) {
      const { streamer } = await ensureInitialized();
      return streamer.listStreamsByRunId(runId);
    },

    // =========================================================================
    // LIFECYCLE
    // =========================================================================
    async start(): Promise<void> {
      const { startQueue } = await ensureInitialized();
      await startQueue();
    },
  };
}

// Default export for WORKFLOW_TARGET_WORLD — the function itself, not a call.
export default createWorld;

export { type AwsConfig } from './aws.js';
export { createStorage, type DynamoStorageConfig } from './storage.js';
export { createQueue, type QueueConfig } from './queue.js';
export { createStreamer, type DynamoStreamerConfig } from './streamer.js';
