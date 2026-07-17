/**
 * DynamoDB Storage Implementation (Event-Sourced, single-table design)
 *
 * All four namespaces (runs, steps, events, hooks) live in one DynamoDB table
 * addressed by a composite PK/SK with a prefix scheme:
 *
 *   Run   PK=RUN#<runId>  SK=RUN#<runId>          GSI1PK=RUNLIST            GSI1SK=<runId>
 *   Step  PK=RUN#<runId>  SK=STEP#<stepId>        GSI1PK=STEP#<stepId>     GSI1SK=STEP#<stepId>
 *   Event PK=RUN#<runId>  SK=EVENT#<eventId>      GSI1PK=CORR#<corrId>?    GSI1SK=<eventId>
 *   Hook  PK=RUN#<runId>  SK=HOOK#<hookId>        GSI1PK=HOOK#<hookId>     GSI1SK=HOOK#<hookId>
 *                                                 GSI2PK=TOKEN#<token>     GSI2SK=TOKEN#<token>
 *
 * GSI1 satisfies: list-all-runs (RUNLIST partition, sorted by runId), step
 * lookup without a runId, hook lookup by hookId, and events-by-correlationId
 * (ascending by eventId). GSI2 satisfies hook lookup by token. Events for a run
 * come back oldest-first because eventIds are monotonic ULIDs and the SK sorts
 * lexicographically (a hard requirement for deterministic replay).
 *
 * Conditional writes (ConditionExpression) enforce idempotent creates.
 */

import {
  RunNotSupportedError,
  WorkflowAPIError,
  WorkflowRunNotFoundError,
} from '@workflow/errors';
import {
  isLegacySpecVersion,
  requiresNewerWorld,
  SPEC_VERSION_CURRENT,
  type AnyEventRequest,
  type CreateEventParams,
  type Event,
  type EventResult,
  type GetHookParams,
  type GetStepParams,
  type GetWorkflowRunParams,
  type Hook,
  type ListEventsByCorrelationIdParams,
  type ListEventsParams,
  type ListHooksParams,
  type ListWorkflowRunStepsParams,
  type ListWorkflowRunsParams,
  type PaginatedResponse,
  type RunCreatedEventRequest,
  type Step,
  type Storage,
  type WorkflowRun,
} from '@workflow/world';
import {
  DeleteCommand,
  GetCommand,
  PutCommand,
  QueryCommand,
  ScanCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';
import { debug, decodeJson, deepClone, encodeJson } from './utils.js';

const generateUlid = monotonicFactory();

// =============================================================================
// Key helpers & item (de)serialization
// =============================================================================

type Entity = 'run' | 'step' | 'event' | 'hook';

interface StoredItem {
  PK: string;
  SK: string;
  GSI1PK?: string;
  GSI1SK?: string;
  GSI2PK?: string;
  GSI2SK?: string;
  entity: Entity;
  runId: string;
  status?: string;
  workflowName?: string;
  correlationId?: string;
  doc: string;
}

const runPK = (runId: string) => `RUN#${runId}`;
const runSK = (runId: string) => `RUN#${runId}`;
const stepSK = (stepId: string) => `STEP#${stepId}`;
const eventSK = (eventId: string) => `EVENT#${eventId}`;
const hookSK = (hookId: string) => `HOOK#${hookId}`;

function runItem(run: WorkflowRun): StoredItem {
  return {
    PK: runPK(run.runId),
    SK: runSK(run.runId),
    GSI1PK: 'RUNLIST',
    GSI1SK: run.runId,
    entity: 'run',
    runId: run.runId,
    status: run.status,
    workflowName: run.workflowName,
    doc: encodeJson(run),
  };
}

function stepItem(step: Step): StoredItem {
  return {
    PK: runPK(step.runId),
    SK: stepSK(step.stepId),
    GSI1PK: stepSK(step.stepId),
    GSI1SK: stepSK(step.stepId),
    entity: 'step',
    runId: step.runId,
    status: step.status,
    doc: encodeJson(step),
  };
}

function eventItem(event: Event): StoredItem {
  const item: StoredItem = {
    PK: runPK(event.runId),
    SK: eventSK(event.eventId),
    entity: 'event',
    runId: event.runId,
    doc: encodeJson(event),
  };
  if (event.correlationId) {
    item.GSI1PK = `CORR#${event.correlationId}`;
    item.GSI1SK = event.eventId;
    item.correlationId = event.correlationId;
  }
  return item;
}

function hookItem(hook: Hook): StoredItem {
  return {
    PK: runPK(hook.runId),
    SK: hookSK(hook.hookId),
    GSI1PK: hookSK(hook.hookId),
    GSI1SK: hookSK(hook.hookId),
    GSI2PK: `TOKEN#${hook.token}`,
    GSI2SK: `TOKEN#${hook.token}`,
    entity: 'hook',
    runId: hook.runId,
    doc: encodeJson(hook),
  };
}

function readDoc<T>(item: Record<string, unknown> | undefined): T | null {
  if (!item || typeof item.doc !== 'string') return null;
  return decodeJson<T>(item.doc);
}

// =============================================================================
// Filtering & pagination
// =============================================================================

function isTerminalRunStatus(status: WorkflowRun['status']): boolean {
  return status === 'completed' || status === 'failed' || status === 'cancelled';
}

function isTerminalStepStatus(status: Step['status']): boolean {
  return status === 'completed' || status === 'failed';
}

function filterRunData(
  run: WorkflowRun,
  resolveData: 'none' | 'all' = 'all'
): WorkflowRun {
  const base = deepClone(run);
  if (resolveData === 'none') {
    return { ...base, input: undefined, output: undefined } as WorkflowRun;
  }
  return base;
}

function filterStepData(step: Step, resolveData: 'none' | 'all' = 'all'): Step {
  const base = deepClone(step);
  if (resolveData === 'none') {
    return { ...base, input: undefined, output: undefined } as Step;
  }
  return base;
}

function filterEventData(event: Event, resolveData: 'none' | 'all' = 'all'): Event {
  const base = deepClone(event);
  if (resolveData === 'none') {
    const { eventData: _eventData, ...rest } = base as Event & {
      eventData?: unknown;
    };
    return rest as Event;
  }
  return base;
}

function filterHookData(hook: Hook, resolveData: 'none' | 'all' = 'all'): Hook {
  const base = deepClone(hook);
  if (resolveData === 'none') {
    const { metadata: _metadata, ...rest } = base as Hook & { metadata?: unknown };
    return rest as Hook;
  }
  return base;
}

function resolveDataOption(params?: CreateEventParams): 'none' | 'all' {
  return params?.resolveData ?? 'all';
}

/**
 * In-memory windowing over a fully-materialized list. Test-scale volumes are
 * small (hundreds of events / dozens of steps per run), so we page through the
 * partition and window in memory to guarantee correct ordering + cursors.
 */
function paginate<T>(
  items: T[],
  getId: (item: T) => string,
  opts: { sortOrder?: 'asc' | 'desc'; limit?: number; cursor?: string },
  defaultSortOrder: 'asc' | 'desc',
  defaultLimit: number
): { data: T[]; cursor: string | null; hasMore: boolean } {
  const sortOrder = opts.sortOrder ?? defaultSortOrder;
  const limit = opts.limit ?? defaultLimit;

  const sorted = [...items].sort((a, b) => {
    const cmp = getId(a).localeCompare(getId(b));
    return sortOrder === 'asc' ? cmp : -cmp;
  });

  let windowed = sorted;
  if (opts.cursor) {
    const idx = sorted.findIndex((item) => getId(item) === opts.cursor);
    if (idx !== -1) windowed = sorted.slice(idx + 1);
  }

  const hasMore = windowed.length > limit;
  const data = windowed.slice(0, limit);
  const cursor = hasMore && data.length > 0 ? getId(data[data.length - 1]) : null;
  return { data, cursor, hasMore };
}

// =============================================================================
// Storage factory
// =============================================================================

export interface DynamoStorageConfig {
  ddb: DynamoDBDocumentClient;
  tableName: string;
}

export function createStorage(config: DynamoStorageConfig): Storage {
  const { ddb, tableName } = config;

  // ---------------------------------------------------------------------------
  // Low-level accessors
  // ---------------------------------------------------------------------------

  async function queryAll(
    input: ConstructorParameters<typeof QueryCommand>[0]
  ): Promise<Record<string, unknown>[]> {
    const items: Record<string, unknown>[] = [];
    let startKey: Record<string, unknown> | undefined;
    do {
      const res = await ddb.send(
        new QueryCommand({ ...input, ExclusiveStartKey: startKey })
      );
      if (res.Items) items.push(...res.Items);
      startKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
    } while (startKey);
    return items;
  }

  async function getRunById(runId: string): Promise<WorkflowRun | null> {
    const res = await ddb.send(
      new GetCommand({
        TableName: tableName,
        Key: { PK: runPK(runId), SK: runSK(runId) },
      })
    );
    return readDoc<WorkflowRun>(res.Item);
  }

  async function putRun(run: WorkflowRun): Promise<void> {
    await ddb.send(new PutCommand({ TableName: tableName, Item: runItem(run) }));
  }

  async function getStepById(runId: string, stepId: string): Promise<Step | null> {
    const res = await ddb.send(
      new GetCommand({
        TableName: tableName,
        Key: { PK: runPK(runId), SK: stepSK(stepId) },
      })
    );
    return readDoc<Step>(res.Item);
  }

  async function getStepByStepId(stepId: string): Promise<Step | null> {
    const items = await queryAll({
      TableName: tableName,
      IndexName: 'GSI1',
      KeyConditionExpression: 'GSI1PK = :pk',
      ExpressionAttributeValues: { ':pk': stepSK(stepId) },
      Limit: 1,
    });
    return items.length ? readDoc<Step>(items[0]) : null;
  }

  async function putStep(step: Step): Promise<void> {
    await ddb.send(new PutCommand({ TableName: tableName, Item: stepItem(step) }));
  }

  async function putEvent(event: Event): Promise<void> {
    await ddb.send(new PutCommand({ TableName: tableName, Item: eventItem(event) }));
  }

  async function getHookById(hookId: string): Promise<Hook | null> {
    const items = await queryAll({
      TableName: tableName,
      IndexName: 'GSI1',
      KeyConditionExpression: 'GSI1PK = :pk',
      ExpressionAttributeValues: { ':pk': hookSK(hookId) },
      Limit: 1,
    });
    return items.length ? readDoc<Hook>(items[0]) : null;
  }

  async function getHookByToken(token: string): Promise<Hook | null> {
    const items = await queryAll({
      TableName: tableName,
      IndexName: 'GSI2',
      KeyConditionExpression: 'GSI2PK = :pk',
      ExpressionAttributeValues: { ':pk': `TOKEN#${token}` },
      Limit: 1,
    });
    return items.length ? readDoc<Hook>(items[0]) : null;
  }

  async function putHook(hook: Hook): Promise<void> {
    await ddb.send(
      new PutCommand({
        TableName: tableName,
        Item: hookItem(hook),
        // Idempotent create: fail if a hook with this id already exists.
        ConditionExpression: 'attribute_not_exists(PK)',
      })
    );
  }

  async function deleteHook(runId: string, hookId: string): Promise<void> {
    await ddb.send(
      new DeleteCommand({
        TableName: tableName,
        Key: { PK: runPK(runId), SK: hookSK(hookId) },
      })
    );
  }

  async function deleteAllHooksForRun(runId: string): Promise<void> {
    const items = await queryAll({
      TableName: tableName,
      KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
      ExpressionAttributeValues: { ':pk': runPK(runId), ':sk': 'HOOK#' },
    });
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new DeleteCommand({
            TableName: tableName,
            Key: { PK: item.PK as string, SK: item.SK as string },
          })
        )
      )
    );
  }

  function isConditionalCheckFailed(err: unknown): boolean {
    return (
      typeof err === 'object' &&
      err !== null &&
      (err as { name?: string }).name === 'ConditionalCheckFailedException'
    );
  }

  // ---------------------------------------------------------------------------
  // Legacy run handling
  // ---------------------------------------------------------------------------

  async function handleLegacyEvent(
    runId: string,
    data: AnyEventRequest,
    currentRun: WorkflowRun,
    params?: CreateEventParams
  ): Promise<EventResult> {
    const resolveData = resolveDataOption(params);

    if (data.eventType === 'run_cancelled') {
      const now = new Date();
      const run: WorkflowRun = {
        ...currentRun,
        status: 'cancelled',
        output: undefined,
        error: undefined,
        completedAt: now,
        updatedAt: now,
      };
      await putRun(run);
      await deleteAllHooksForRun(runId);
      return { event: undefined, run: filterRunData(run, resolveData) };
    }

    if (data.eventType === 'wait_completed' || data.eventType === 'hook_received') {
      const event: Event = {
        ...data,
        runId,
        eventId: `evnt_${generateUlid()}`,
        createdAt: new Date(),
        specVersion: SPEC_VERSION_CURRENT,
      } as Event;
      await putEvent(event);
      return { event: filterEventData(event, resolveData) };
    }

    throw new WorkflowAPIError(
      `Event '${data.eventType}' is not supported for legacy runs`,
      { status: 409 }
    );
  }

  // ---------------------------------------------------------------------------
  // Storage interface
  // ---------------------------------------------------------------------------

  const storage = {
    runs: {
      async get(id: string, params?: GetWorkflowRunParams) {
        const run = await getRunById(id);
        if (!run) throw new WorkflowRunNotFoundError(id);
        return filterRunData(run, params?.resolveData);
      },

      async list(
        params?: ListWorkflowRunsParams
      ): Promise<PaginatedResponse<WorkflowRun>> {
        const items = await queryAll({
          TableName: tableName,
          IndexName: 'GSI1',
          KeyConditionExpression: 'GSI1PK = :pk',
          ExpressionAttributeValues: { ':pk': 'RUNLIST' },
        });

        let runs = items
          .map((item) => readDoc<WorkflowRun>(item))
          .filter((r): r is WorkflowRun => r !== null);

        if (params?.workflowName) {
          runs = runs.filter((r) => r.workflowName === params.workflowName);
        }
        if (params?.status) {
          runs = runs.filter((r) => r.status === params.status);
        }

        const page = paginate(
          runs,
          (r) => r.runId,
          {
            sortOrder: params?.pagination?.sortOrder,
            limit: params?.pagination?.limit,
            cursor: params?.pagination?.cursor,
          },
          'desc',
          100
        );

        return {
          data: page.data.map((r) => filterRunData(r, params?.resolveData)),
          cursor: page.cursor,
          hasMore: page.hasMore,
        };
      },
    },

    steps: {
      async get(
        runId: string | undefined,
        stepId: string,
        params?: GetStepParams
      ) {
        const step = runId
          ? await getStepById(runId, stepId)
          : await getStepByStepId(stepId);
        if (!step) {
          throw new WorkflowAPIError(`Step not found: ${stepId}`, { status: 404 });
        }
        return filterStepData(step, params?.resolveData);
      },

      async list(
        params: ListWorkflowRunStepsParams
      ): Promise<PaginatedResponse<Step>> {
        const items = await queryAll({
          TableName: tableName,
          KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
          ExpressionAttributeValues: {
            ':pk': runPK(params.runId),
            ':sk': 'STEP#',
          },
        });

        const steps = items
          .map((item) => readDoc<Step>(item))
          .filter((s): s is Step => s !== null);

        const page = paginate(
          steps,
          (s) => s.stepId,
          {
            sortOrder: params.pagination?.sortOrder,
            limit: params.pagination?.limit,
            cursor: params.pagination?.cursor,
          },
          'desc',
          100
        );

        return {
          data: page.data.map((s) => filterStepData(s, params.resolveData)),
          cursor: page.cursor,
          hasMore: page.hasMore,
        };
      },
    },

    events: {
      async create(
        runId: string | null,
        data: AnyEventRequest,
        params?: CreateEventParams
      ): Promise<EventResult> {
        const now = new Date();
        const resolveData = resolveDataOption(params);
        const effectiveSpecVersion = data.specVersion ?? SPEC_VERSION_CURRENT;
        const eventId = `evnt_${generateUlid()}`;

        let effectiveRunId: string;
        if (data.eventType === 'run_created') {
          effectiveRunId = runId ?? `wrun_${generateUlid()}`;
        } else {
          if (!runId) {
            throw new WorkflowAPIError(
              'runId is required for non run_created events',
              { status: 400 }
            );
          }
          effectiveRunId = runId;
        }

        const currentRun = await getRunById(effectiveRunId);

        if (currentRun) {
          if (requiresNewerWorld(currentRun.specVersion)) {
            throw new RunNotSupportedError(
              currentRun.specVersion as number,
              SPEC_VERSION_CURRENT
            );
          }

          if (isLegacySpecVersion(currentRun.specVersion)) {
            return handleLegacyEvent(effectiveRunId, data, currentRun, params);
          }

          if (isTerminalRunStatus(currentRun.status)) {
            const runTerminalEvents = new Set([
              'run_started',
              'run_completed',
              'run_failed',
              'run_cancelled',
            ]);

            if (
              data.eventType === 'run_cancelled' &&
              currentRun.status === 'cancelled'
            ) {
              const idempotentEvent: Event = {
                ...data,
                runId: effectiveRunId,
                eventId,
                createdAt: now,
                specVersion: effectiveSpecVersion,
              } as Event;
              await putEvent(idempotentEvent);
              return {
                event: filterEventData(idempotentEvent, resolveData),
                run: filterRunData(currentRun, resolveData),
              };
            }

            if (runTerminalEvents.has(data.eventType)) {
              throw new WorkflowAPIError(
                `Cannot transition run from terminal state '${currentRun.status}'`,
                { status: 409 }
              );
            }

            if (
              data.eventType === 'step_created' ||
              data.eventType === 'hook_created'
            ) {
              throw new WorkflowAPIError(
                `Cannot create entities on terminal run '${currentRun.status}'`,
                { status: 409 }
              );
            }
          }
        }

        if (!currentRun && data.eventType !== 'run_created') {
          throw new WorkflowAPIError(`Run not found: ${effectiveRunId}`, {
            status: 404,
          });
        }

        let validatedStep: Step | null = null;
        const stepEvents = new Set([
          'step_started',
          'step_completed',
          'step_failed',
          'step_retrying',
        ]);

        if (stepEvents.has(data.eventType)) {
          if (!data.correlationId) {
            throw new WorkflowAPIError('Step events require correlationId', {
              status: 400,
            });
          }

          validatedStep = await getStepById(effectiveRunId, data.correlationId);
          if (!validatedStep) {
            throw new WorkflowAPIError(`Step '${data.correlationId}' not found`, {
              status: 404,
            });
          }

          if (isTerminalStepStatus(validatedStep.status)) {
            throw new WorkflowAPIError(
              `Cannot modify step in terminal state '${validatedStep.status}'`,
              { status: 409 }
            );
          }

          if (
            currentRun &&
            isTerminalRunStatus(currentRun.status) &&
            validatedStep.status !== 'running'
          ) {
            throw new WorkflowAPIError(
              `Cannot modify non-running step on terminal run '${currentRun.status}'`,
              { status: 410 }
            );
          }
        }

        if (
          (data.eventType === 'hook_received' ||
            data.eventType === 'hook_disposed') &&
          data.correlationId
        ) {
          const existingHook = await getHookById(data.correlationId);
          if (!existingHook) {
            throw new WorkflowAPIError(`Hook '${data.correlationId}' not found`, {
              status: 404,
            });
          }
        }

        let run: WorkflowRun | undefined;
        let step: Step | undefined;
        let hook: Hook | undefined;

        if (data.eventType === 'run_created') {
          const runData = (data as RunCreatedEventRequest).eventData;
          run = {
            runId: effectiveRunId,
            deploymentId: runData.deploymentId,
            workflowName: runData.workflowName,
            status: 'pending',
            specVersion: effectiveSpecVersion,
            executionContext: runData.executionContext,
            input: runData.input,
            output: undefined,
            error: undefined,
            startedAt: undefined,
            completedAt: undefined,
            createdAt: now,
            updatedAt: now,
          } as WorkflowRun;
          await putRun(run);
        } else if (data.eventType === 'run_started') {
          if (!currentRun) {
            throw new WorkflowAPIError(`Run not found: ${effectiveRunId}`, {
              status: 404,
            });
          }
          run = {
            ...currentRun,
            status: 'running',
            // startedAt is set once, only on first transition to running.
            startedAt: currentRun.startedAt ?? now,
            output: undefined,
            error: undefined,
            completedAt: undefined,
            updatedAt: now,
          };
          await putRun(run);
        } else if (data.eventType === 'run_completed') {
          if (!currentRun) {
            throw new WorkflowAPIError(`Run not found: ${effectiveRunId}`, {
              status: 404,
            });
          }
          run = {
            ...currentRun,
            status: 'completed',
            output: data.eventData?.output,
            error: undefined,
            completedAt: now,
            updatedAt: now,
          };
          await putRun(run);
          await deleteAllHooksForRun(run.runId);
        } else if (data.eventType === 'run_failed') {
          if (!currentRun) {
            throw new WorkflowAPIError(`Run not found: ${effectiveRunId}`, {
              status: 404,
            });
          }
          run = {
            ...currentRun,
            status: 'failed',
            output: undefined,
            error: {
              message:
                typeof data.eventData.error === 'string'
                  ? data.eventData.error
                  : (data.eventData.error?.message ?? 'Unknown error'),
              stack: data.eventData.error?.stack,
              code: data.eventData.errorCode,
            },
            completedAt: now,
            updatedAt: now,
          };
          await putRun(run);
          await deleteAllHooksForRun(run.runId);
        } else if (data.eventType === 'run_cancelled') {
          if (!currentRun) {
            throw new WorkflowAPIError(`Run not found: ${effectiveRunId}`, {
              status: 404,
            });
          }
          run = {
            ...currentRun,
            status: 'cancelled',
            output: undefined,
            error: undefined,
            completedAt: now,
            updatedAt: now,
          };
          await putRun(run);
          await deleteAllHooksForRun(run.runId);
        } else if (data.eventType === 'step_created') {
          if (!data.correlationId) {
            throw new WorkflowAPIError('Step events require correlationId', {
              status: 400,
            });
          }
          step = {
            runId: effectiveRunId,
            stepId: data.correlationId,
            stepName: data.eventData.stepName,
            status: 'pending',
            input: data.eventData.input,
            output: undefined,
            error: undefined,
            attempt: 0,
            startedAt: undefined,
            completedAt: undefined,
            createdAt: now,
            updatedAt: now,
            retryAfter: undefined,
            specVersion: effectiveSpecVersion,
          } as Step;
          try {
            await ddb.send(
              new PutCommand({
                TableName: tableName,
                Item: stepItem(step),
                ConditionExpression:
                  'attribute_not_exists(PK) AND attribute_not_exists(SK)',
              })
            );
          } catch (err) {
            if (isConditionalCheckFailed(err)) {
              throw new WorkflowAPIError(
                `Step '${data.correlationId}' already exists`,
                { status: 409 }
              );
            }
            throw err;
          }
        } else if (data.eventType === 'step_started') {
          if (!validatedStep || !data.correlationId) {
            throw new WorkflowAPIError('Step not found for step_started', {
              status: 404,
            });
          }

          if (
            validatedStep.retryAfter &&
            validatedStep.retryAfter.getTime() > Date.now()
          ) {
            const err = new WorkflowAPIError(
              `Cannot start step '${data.correlationId}' before retryAfter`,
              { status: 425 }
            );
            (err as WorkflowAPIError & { meta?: Record<string, string> }).meta = {
              stepId: data.correlationId,
              retryAfter: validatedStep.retryAfter.toISOString(),
            };
            throw err;
          }

          step = {
            ...validatedStep,
            status: 'running',
            startedAt: validatedStep.startedAt ?? now,
            attempt: validatedStep.attempt + 1,
            retryAfter: undefined,
            updatedAt: now,
          };
          await putStep(step);
        } else if (data.eventType === 'step_completed') {
          if (!validatedStep) {
            throw new WorkflowAPIError('Step not found for step_completed', {
              status: 404,
            });
          }
          step = {
            ...validatedStep,
            status: 'completed',
            output: data.eventData.result,
            completedAt: now,
            updatedAt: now,
          };
          await putStep(step);
        } else if (data.eventType === 'step_failed') {
          if (!validatedStep) {
            throw new WorkflowAPIError('Step not found for step_failed', {
              status: 404,
            });
          }
          step = {
            ...validatedStep,
            status: 'failed',
            error: {
              message:
                typeof data.eventData.error === 'string'
                  ? data.eventData.error
                  : (data.eventData.error?.message ?? 'Unknown error'),
              stack: data.eventData.stack,
            },
            completedAt: now,
            updatedAt: now,
          };
          await putStep(step);
        } else if (data.eventType === 'step_retrying') {
          if (!validatedStep) {
            throw new WorkflowAPIError('Step not found for step_retrying', {
              status: 404,
            });
          }
          step = {
            ...validatedStep,
            status: 'pending',
            error: {
              message:
                typeof data.eventData.error === 'string'
                  ? data.eventData.error
                  : (data.eventData.error?.message ?? 'Unknown error'),
              stack: data.eventData.stack,
            },
            retryAfter: data.eventData.retryAfter,
            updatedAt: now,
          };
          await putStep(step);
        } else if (data.eventType === 'hook_created') {
          const existingByToken = await getHookByToken(data.eventData.token);
          if (existingByToken) {
            const conflictEvent: Event = {
              eventType: 'hook_conflict',
              correlationId: data.correlationId,
              eventData: { token: data.eventData.token },
              runId: effectiveRunId,
              eventId,
              createdAt: now,
              specVersion: effectiveSpecVersion,
            } as Event;
            await putEvent(conflictEvent);
            return {
              event: filterEventData(conflictEvent, resolveData),
              run,
              step,
              hook: undefined,
            };
          }

          const existingById = await getHookById(data.correlationId);
          if (existingById) {
            throw new WorkflowAPIError(
              `Hook '${data.correlationId}' already exists`,
              { status: 409 }
            );
          }

          hook = {
            runId: effectiveRunId,
            hookId: data.correlationId,
            token: data.eventData.token,
            metadata: data.eventData.metadata,
            ownerId: 'aws-owner',
            projectId: 'aws-project',
            environment: 'development',
            createdAt: now,
            specVersion: effectiveSpecVersion,
          } as Hook;
          try {
            await putHook(hook);
          } catch (err) {
            if (isConditionalCheckFailed(err)) {
              const conflictEvent: Event = {
                eventType: 'hook_conflict',
                correlationId: data.correlationId,
                eventData: { token: data.eventData.token },
                runId: effectiveRunId,
                eventId,
                createdAt: now,
                specVersion: effectiveSpecVersion,
              } as Event;
              await putEvent(conflictEvent);
              return {
                event: filterEventData(conflictEvent, resolveData),
                run,
                step,
                hook: undefined,
              };
            }
            throw err;
          }
        } else if (data.eventType === 'hook_disposed') {
          if (data.correlationId) {
            await deleteHook(effectiveRunId, data.correlationId);
          }
        }

        const event: Event = {
          ...data,
          runId: effectiveRunId,
          eventId,
          createdAt: now,
          specVersion: effectiveSpecVersion,
        } as Event;

        await putEvent(event);

        return {
          event: filterEventData(event, resolveData),
          run: run ? deepClone(run) : undefined,
          step: step ? deepClone(step) : undefined,
          hook: hook ? deepClone(hook) : undefined,
        };
      },

      async list(params: ListEventsParams): Promise<PaginatedResponse<Event>> {
        const items = await queryAll({
          TableName: tableName,
          KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
          ExpressionAttributeValues: {
            ':pk': runPK(params.runId),
            ':sk': 'EVENT#',
          },
          // ScanIndexForward true keeps DynamoDB pages ascending; the in-memory
          // paginate() below re-sorts to the requested order regardless.
          ScanIndexForward: true,
        });

        const events = items
          .map((item) => readDoc<Event>(item))
          .filter((e): e is Event => e !== null);

        const page = paginate(
          events,
          (e) => e.eventId,
          {
            sortOrder: params.pagination?.sortOrder,
            limit: params.pagination?.limit,
            cursor: params.pagination?.cursor,
          },
          'asc',
          10000
        );

        return {
          data: page.data.map((e) => filterEventData(e, params.resolveData)),
          cursor: page.cursor,
          hasMore: page.hasMore,
        };
      },

      async listByCorrelationId(
        params: ListEventsByCorrelationIdParams
      ): Promise<PaginatedResponse<Event>> {
        const items = await queryAll({
          TableName: tableName,
          IndexName: 'GSI1',
          KeyConditionExpression: 'GSI1PK = :pk',
          ExpressionAttributeValues: { ':pk': `CORR#${params.correlationId}` },
          ScanIndexForward: true,
        });

        const events = items
          .map((item) => readDoc<Event>(item))
          .filter((e): e is Event => e !== null);

        const page = paginate(
          events,
          (e) => e.eventId,
          {
            sortOrder: params.pagination?.sortOrder,
            limit: params.pagination?.limit,
            cursor: params.pagination?.cursor,
          },
          'asc',
          100
        );

        return {
          data: page.data.map((e) => filterEventData(e, params.resolveData)),
          cursor: page.cursor,
          hasMore: page.hasMore,
        };
      },
    },

    hooks: {
      async get(hookId: string, params?: GetHookParams) {
        const hook = await getHookById(hookId);
        if (!hook) {
          throw new WorkflowAPIError(`Hook not found: ${hookId}`, { status: 404 });
        }
        return filterHookData(hook, params?.resolveData);
      },

      async getByToken(token: string, params?: GetHookParams) {
        const hook = await getHookByToken(token);
        if (!hook) {
          throw new WorkflowAPIError(`Hook not found for token: ${token}`, {
            status: 404,
          });
        }
        return filterHookData(hook, params?.resolveData);
      },

      async list(params: ListHooksParams): Promise<PaginatedResponse<Hook>> {
        let items: Record<string, unknown>[];
        if (params.runId) {
          items = await queryAll({
            TableName: tableName,
            KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
            ExpressionAttributeValues: {
              ':pk': runPK(params.runId),
              ':sk': 'HOOK#',
            },
          });
        } else {
          // No runId: hooks live under many partitions, so fall back to a scan
          // (rare, low-volume path).
          const scanItems: Record<string, unknown>[] = [];
          let startKey: Record<string, unknown> | undefined;
          do {
            const res = await ddb.send(
              new ScanCommand({
                TableName: tableName,
                FilterExpression: 'entity = :e',
                ExpressionAttributeValues: { ':e': 'hook' },
                ExclusiveStartKey: startKey,
              })
            );
            if (res.Items) scanItems.push(...res.Items);
            startKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
          } while (startKey);
          items = scanItems;
        }

        const hooks = items
          .map((item) => readDoc<Hook>(item))
          .filter((h): h is Hook => h !== null);

        const page = paginate(
          hooks,
          (h) => h.hookId,
          {
            sortOrder: params.pagination?.sortOrder,
            limit: params.pagination?.limit,
            cursor: params.pagination?.cursor,
          },
          'desc',
          100
        );

        return {
          data: page.data.map((h) => filterHookData(h, params.resolveData)),
          cursor: page.cursor,
          hasMore: page.hasMore,
        };
      },
    },
  };

  debug('DynamoDB storage initialized on table:', tableName);
  return storage as unknown as Storage;
}
