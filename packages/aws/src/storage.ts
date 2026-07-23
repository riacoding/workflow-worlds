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
  EntityConflictError,
  HookNotFoundError,
  RunNotSupportedError,
  WorkflowWorldError,
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
  type GetEventParams,
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
  TransactWriteCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';
import { cborDecode, cborEncode } from './cbor.js';
import { debug, decodeJson, deepClone, encodeJson } from './utils.js';

const generateUlid = monotonicFactory();

// =============================================================================
// Key helpers & item (de)serialization
// =============================================================================

type Entity = 'run' | 'step' | 'event' | 'hook' | 'hook_token';

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
  // Spec-version-3 payload fields, stored as raw CBOR bytes (see docs/Schema-spec3-update.md).
  // The equivalently-named plain JSON attribute (input, output, executionContext, error,
  // metadata, payload) is reserved for a future legacy-fallback read path but is never written
  // or read by this implementation — there is no pre-existing data to stay compatible with.
  inputCbor?: Uint8Array;
  outputCbor?: Uint8Array;
  executionContextCbor?: Uint8Array;
  errorCbor?: Uint8Array;
  metadataCbor?: Uint8Array;
  payloadCbor?: Uint8Array;
}

const runPK = (runId: string) => `RUN#${runId}`;
const runSK = (runId: string) => `RUN#${runId}`;
const stepSK = (stepId: string) => `STEP#${stepId}`;
const eventSK = (eventId: string) => `EVENT#${eventId}`;
const hookSK = (hookId: string) => `HOOK#${hookId}`;
const tokenKey = (token: string) => `TOKEN#${token}`;

function runItem(run: WorkflowRun): StoredItem {
  const { input, output, executionContext, error, ...envelope } = run;
  return {
    PK: runPK(run.runId),
    SK: runSK(run.runId),
    GSI1PK: 'RUNLIST',
    GSI1SK: run.runId,
    entity: 'run',
    runId: run.runId,
    status: run.status,
    workflowName: run.workflowName,
    doc: encodeJson(envelope),
    inputCbor: cborEncode(input),
    outputCbor: cborEncode(output),
    executionContextCbor: cborEncode(executionContext),
    errorCbor: cborEncode(error),
  };
}

function stepItem(step: Step): StoredItem {
  const { input, output, error, ...envelope } = step;
  return {
    PK: runPK(step.runId),
    SK: stepSK(step.stepId),
    GSI1PK: stepSK(step.stepId),
    GSI1SK: stepSK(step.stepId),
    entity: 'step',
    runId: step.runId,
    status: step.status,
    doc: encodeJson(envelope),
    inputCbor: cborEncode(input),
    outputCbor: cborEncode(output),
    errorCbor: cborEncode(error),
  };
}

function eventItem(event: Event): StoredItem {
  const { eventData, ...envelope } = event as Event & { eventData?: unknown };
  const item: StoredItem = {
    PK: runPK(event.runId),
    SK: eventSK(event.eventId),
    entity: 'event',
    runId: event.runId,
    doc: encodeJson(envelope),
    payloadCbor: cborEncode(eventData),
  };
  if (event.correlationId) {
    item.GSI1PK = `CORR#${event.correlationId}`;
    item.GSI1SK = event.eventId;
    item.correlationId = event.correlationId;
  }
  return item;
}

function hookItem(hook: Hook): StoredItem {
  const { metadata, ...envelope } = hook;
  return {
    PK: runPK(hook.runId),
    SK: hookSK(hook.hookId),
    GSI1PK: hookSK(hook.hookId),
    GSI1SK: hookSK(hook.hookId),
    GSI2PK: tokenKey(hook.token),
    GSI2SK: tokenKey(hook.token),
    entity: 'hook',
    runId: hook.runId,
    doc: encodeJson(envelope),
    metadataCbor: cborEncode(metadata),
  };
}

// A standalone item whose PK is the token itself, used purely to reserve
// token uniqueness with a conditional write. The hook item's own
// ConditionExpression only guards its own PK/SK (RUN#<runId>/HOOK#<hookId>),
// so two different hookIds can otherwise both succeed in claiming the same
// token — the only thing that previously caught that was a preceding read
// against GSI2, which is eventually consistent. This item lets the token
// claim itself be part of the same atomic TransactWriteCommand as the hook
// write (see putHook).
function hookTokenReservationItem(hook: Hook): StoredItem {
  return {
    PK: tokenKey(hook.token),
    SK: tokenKey(hook.token),
    entity: 'hook_token',
    runId: hook.runId,
    doc: encodeJson({ runId: hook.runId, hookId: hook.hookId }),
  };
}

// NOTE: each field below is only overwritten when its *Cbor attribute is
// actually present. Rows written before this change (or by anything that
// hasn't cut over yet) still carry these fields inline in `doc` — falling
// back to the decoded envelope value instead of blindly assigning
// `cborDecode(undefined)` (=== undefined) avoids silently wiping out data
// that's still perfectly readable on disk.

function readRun(item: Record<string, unknown> | undefined): WorkflowRun | null {
  if (!item || typeof item.doc !== 'string') return null;
  const run = decodeJson<WorkflowRun>(item.doc);
  const input = cborDecode<WorkflowRun['input']>(item.inputCbor as Uint8Array | undefined);
  if (input !== undefined) run.input = input;
  const output = cborDecode<WorkflowRun['output']>(item.outputCbor as Uint8Array | undefined);
  if (output !== undefined) run.output = output;
  const executionContext = cborDecode<WorkflowRun['executionContext']>(
    item.executionContextCbor as Uint8Array | undefined
  );
  if (executionContext !== undefined) run.executionContext = executionContext;
  const error = cborDecode<WorkflowRun['error']>(item.errorCbor as Uint8Array | undefined);
  if (error !== undefined) run.error = error;
  return run;
}

function readStep(item: Record<string, unknown> | undefined): Step | null {
  if (!item || typeof item.doc !== 'string') return null;
  const step = decodeJson<Step>(item.doc);
  const input = cborDecode<Step['input']>(item.inputCbor as Uint8Array | undefined);
  if (input !== undefined) step.input = input;
  const output = cborDecode<Step['output']>(item.outputCbor as Uint8Array | undefined);
  if (output !== undefined) step.output = output;
  const error = cborDecode<Step['error']>(item.errorCbor as Uint8Array | undefined);
  if (error !== undefined) step.error = error;
  return step;
}

function readHook(item: Record<string, unknown> | undefined): Hook | null {
  if (!item || typeof item.doc !== 'string') return null;
  const hook = decodeJson<Hook>(item.doc);
  const metadata = cborDecode<Hook['metadata']>(item.metadataCbor as Uint8Array | undefined);
  if (metadata !== undefined) hook.metadata = metadata;
  return hook;
}

function readEvent(item: Record<string, unknown> | undefined): Event | null {
  if (!item || typeof item.doc !== 'string') return null;
  const envelope = decodeJson<Event>(item.doc);
  const eventData = cborDecode(item.payloadCbor as Uint8Array | undefined);
  if (eventData === undefined) return envelope;
  return { ...envelope, eventData } as Event;
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
    return readRun(res.Item);
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
    return readStep(res.Item);
  }

  async function getStepByStepId(stepId: string): Promise<Step | null> {
    const items = await queryAll({
      TableName: tableName,
      IndexName: 'GSI1',
      KeyConditionExpression: 'GSI1PK = :pk',
      ExpressionAttributeValues: { ':pk': stepSK(stepId) },
      Limit: 1,
    });
    return items.length ? readStep(items[0]) : null;
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
    return items.length ? readHook(items[0]) : null;
  }

  async function getHookByToken(token: string): Promise<Hook | null> {
    const items = await queryAll({
      TableName: tableName,
      IndexName: 'GSI2',
      KeyConditionExpression: 'GSI2PK = :pk',
      ExpressionAttributeValues: { ':pk': tokenKey(token) },
      Limit: 1,
    });
    return items.length ? readHook(items[0]) : null;
  }

  /**
   * Reads the token-reservation item directly by its own PK/SK (a plain
   * GetCommand, strongly consistent) rather than via GSI2 (eventually
   * consistent). Used right after losing a putHook token race, when we need
   * the winning hook's runId for the hook_conflict event's conflictingRunId
   * and can't afford to read a stale/empty GSI2 result.
   */
  async function getHookTokenReservation(
    token: string
  ): Promise<{ runId: string; hookId: string } | null> {
    const res = await ddb.send(
      new GetCommand({
        TableName: tableName,
        Key: { PK: tokenKey(token), SK: tokenKey(token) },
      })
    );
    if (!res.Item || typeof res.Item.doc !== 'string') return null;
    return decodeJson<{ runId: string; hookId: string }>(res.Item.doc);
  }

  /**
   * Creates the hook item and its token-reservation item atomically. Both
   * writes are conditioned on attribute_not_exists(PK): the hook write
   * guards against a duplicate hookId, the reservation write guards against
   * a duplicate token. Doing this in one transaction (rather than a
   * getHookByToken() read followed by a plain put) closes the race where two
   * different hookIds claim the same token in the same tick — the read is
   * against GSI2, which is only eventually consistent.
   *
   * Throws the raw DynamoDB error on failure; callers should inspect it with
   * `hookConflictKind()` to tell a token conflict from a hookId conflict.
   */
  async function putHook(hook: Hook): Promise<void> {
    await ddb.send(
      new TransactWriteCommand({
        TransactItems: [
          {
            Put: {
              TableName: tableName,
              Item: hookItem(hook),
              ConditionExpression: 'attribute_not_exists(PK)',
            },
          },
          {
            Put: {
              TableName: tableName,
              Item: hookTokenReservationItem(hook),
              ConditionExpression: 'attribute_not_exists(PK)',
            },
          },
        ],
      })
    );
  }

  /** Which TransactWriteCommand item in putHook's transaction failed its condition. */
  function hookConflictKind(err: unknown): 'hookId' | 'token' | null {
    if (
      typeof err !== 'object' ||
      err === null ||
      (err as { name?: string }).name !== 'TransactionCanceledException'
    ) {
      return null;
    }
    const reasons = (err as { CancellationReasons?: { Code?: string }[] })
      .CancellationReasons;
    if (reasons?.[1]?.Code === 'ConditionalCheckFailed') return 'token';
    if (reasons?.[0]?.Code === 'ConditionalCheckFailed') return 'hookId';
    return null;
  }

  async function hookCreatedEventExists(correlationId: string): Promise<boolean> {
    const items = await queryAll({
      TableName: tableName,
      IndexName: 'GSI1',
      KeyConditionExpression: 'GSI1PK = :pk',
      ExpressionAttributeValues: { ':pk': `CORR#${correlationId}` },
    });
    return items.some((item) => readEvent(item)?.eventType === 'hook_created');
  }

  /**
   * Called when a hook_created call finds that `owner` (a hook found either
   * by token pre-check or by losing putHook's transaction) already claims the
   * hookId/token being created. Same (runId, hookId) as the caller's own
   * request means this is either an idempotent replay of the exact same
   * hook_created call (queue at-least-once delivery, or the immediate
   * re-invocation used for hook-conflict/getConflict signaling) or a
   * crash-recovered orphaned hook row — the entity write and the event write
   * are not atomic here, so a crash between them can leave a hook with no
   * hook_created event in the log yet. Distinguish by checking the log:
   *   - event exists  → real duplicate processing: throw EntityConflictError
   *     so the runtime's concurrent-replay catch path (suspension-handler.ts's
   *     createHookEvent) swallows it, instead of writing a self-conflict that
   *     would later replay as a HookConflictError against its own run.
   *   - event missing → orphaned row: return it so the caller skips
   *     re-creating the hook and falls through to complete the write.
   * Returns null when `owner` is a genuinely different (runId, hookId) —
   * the caller should treat that as a real conflict. Mirrors
   * @workflow/world-postgres's handling of the same race
   * (see vercel/workflow#2283).
   */
  async function resolveHookCreateConflict(
    owner: { runId: string; hookId: string },
    effectiveRunId: string,
    correlationId: string
  ): Promise<Hook | null> {
    if (owner.runId !== effectiveRunId || owner.hookId !== correlationId) {
      return null;
    }
    if (await hookCreatedEventExists(correlationId)) {
      throw new EntityConflictError(`Hook '${correlationId}' already created`);
    }
    return getHookById(correlationId);
  }

  async function deleteHook(runId: string, hookId: string, token: string): Promise<void> {
    await Promise.all([
      ddb.send(
        new DeleteCommand({
          TableName: tableName,
          Key: { PK: runPK(runId), SK: hookSK(hookId) },
        })
      ),
      ddb.send(
        new DeleteCommand({
          TableName: tableName,
          Key: { PK: tokenKey(token), SK: tokenKey(token) },
        })
      ),
    ]);
  }

  async function deleteAllHooksForRun(runId: string): Promise<void> {
    const items = await queryAll({
      TableName: tableName,
      KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
      ExpressionAttributeValues: { ':pk': runPK(runId), ':sk': 'HOOK#' },
    });
    await Promise.all(
      items.flatMap((item) => {
        const deletes = [
          ddb.send(
            new DeleteCommand({
              TableName: tableName,
              Key: { PK: item.PK as string, SK: item.SK as string },
            })
          ),
        ];
        // Each hook item's GSI2PK/GSI2SK equal its token reservation item's
        // own PK/SK (see hookTokenReservationItem) — reuse them here instead
        // of re-decoding the token out of `doc`.
        if (item.GSI2PK && item.GSI2SK) {
          deletes.push(
            ddb.send(
              new DeleteCommand({
                TableName: tableName,
                Key: { PK: item.GSI2PK as string, SK: item.GSI2SK as string },
              })
            )
          );
        }
        return deletes;
      })
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

    throw new WorkflowWorldError(
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
          .map((item) => readRun(item))
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
          throw new WorkflowWorldError(`Step not found: ${stepId}`, { status: 404 });
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
          .map((item) => readStep(item))
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
            throw new WorkflowWorldError(
              'runId is required for non run_created events',
              { status: 400 }
            );
          }
          effectiveRunId = runId;
        }

        let currentRun = await getRunById(effectiveRunId);

        // Resilient start: if the original run_created event failed to persist
        // (e.g. a transient error during start()), the queued run_started event
        // carries the original run input so the run can be bootstrapped here
        // instead of failing outright — start() already accepted the run via
        // the queue and told the caller creation would be retried async.
        // Mirrors @workflow/world-postgres's handling of the same contract.
        if (!currentRun && data.eventType === 'run_started' && data.eventData) {
          const { deploymentId, workflowName, input, executionContext } = data.eventData;
          if (deploymentId && workflowName && input !== undefined) {
            const bootstrapRun: WorkflowRun = {
              runId: effectiveRunId,
              deploymentId,
              workflowName,
              status: 'pending',
              specVersion: effectiveSpecVersion,
              executionContext,
              input,
              output: undefined,
              error: undefined,
              startedAt: undefined,
              completedAt: undefined,
              createdAt: now,
              updatedAt: now,
            } as WorkflowRun;
            try {
              await ddb.send(
                new PutCommand({
                  TableName: tableName,
                  Item: runItem(bootstrapRun),
                  // Idempotent create: fail if a concurrent bootstrap/run_created won the race.
                  ConditionExpression: 'attribute_not_exists(PK)',
                })
              );
              const runCreatedEvent: Event = {
                eventType: 'run_created',
                runId: effectiveRunId,
                eventId: `evnt_${generateUlid()}`,
                createdAt: now,
                specVersion: effectiveSpecVersion,
                eventData: { deploymentId, workflowName, input, executionContext },
              } as Event;
              await putEvent(runCreatedEvent);
              currentRun = bootstrapRun;
            } catch (err) {
              if (!isConditionalCheckFailed(err)) throw err;
              // Lost the race to a concurrent run_created/bootstrap — re-read.
              currentRun = await getRunById(effectiveRunId);
            }
          }
        }

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
              throw new WorkflowWorldError(
                `Cannot transition run from terminal state '${currentRun.status}'`,
                { status: 409 }
              );
            }

            if (
              data.eventType === 'step_created' ||
              data.eventType === 'hook_created'
            ) {
              throw new WorkflowWorldError(
                `Cannot create entities on terminal run '${currentRun.status}'`,
                { status: 409 }
              );
            }
          }
        }

        if (!currentRun && data.eventType !== 'run_created') {
          throw new WorkflowWorldError(`Run not found: ${effectiveRunId}`, {
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
            throw new WorkflowWorldError('Step events require correlationId', {
              status: 400,
            });
          }

          validatedStep = await getStepById(effectiveRunId, data.correlationId);
          if (!validatedStep) {
            throw new WorkflowWorldError(`Step '${data.correlationId}' not found`, {
              status: 404,
            });
          }

          if (isTerminalStepStatus(validatedStep.status)) {
            throw new WorkflowWorldError(
              `Cannot modify step in terminal state '${validatedStep.status}'`,
              { status: 409 }
            );
          }

          if (
            currentRun &&
            isTerminalRunStatus(currentRun.status) &&
            validatedStep.status !== 'running'
          ) {
            throw new WorkflowWorldError(
              `Cannot modify non-running step on terminal run '${currentRun.status}'`,
              { status: 410 }
            );
          }
        }

        let hookForDisposal: Hook | null = null;
        if (
          (data.eventType === 'hook_received' ||
            data.eventType === 'hook_disposed') &&
          data.correlationId
        ) {
          const existingHook = await getHookById(data.correlationId);
          if (!existingHook) {
            throw new WorkflowWorldError(`Hook '${data.correlationId}' not found`, {
              status: 404,
            });
          }
          hookForDisposal = existingHook;
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
            throw new WorkflowWorldError(`Run not found: ${effectiveRunId}`, {
              status: 404,
            });
          }
          // Idempotent for redeliveries: a run that's already running just
          // returns as-is, with no new run_started event written. Besides
          // avoiding a pointless write, this matters for replay determinism —
          // core's deterministic workflow clock advances via each consumed
          // event's createdAt (see events-consumer.ts), so a fresh event here
          // on every redelivery would re-pin the clock to "now" right before
          // replayed workflow code re-reads Date.now(), making elapsed-time
          // measurements across a suspend/resume collapse to a few ms
          // regardless of how long the run actually waited. Matches
          // @workflow/world-postgres's handling of the same event.
          if (currentRun.status === 'running') {
            return { run: filterRunData(currentRun, resolveData) };
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
            throw new WorkflowWorldError(`Run not found: ${effectiveRunId}`, {
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
            throw new WorkflowWorldError(`Run not found: ${effectiveRunId}`, {
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
            throw new WorkflowWorldError(`Run not found: ${effectiveRunId}`, {
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
            throw new WorkflowWorldError('Step events require correlationId', {
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
              throw new WorkflowWorldError(
                `Step '${data.correlationId}' already exists`,
                { status: 409 }
              );
            }
            throw err;
          }
        } else if (data.eventType === 'step_started') {
          if (!validatedStep || !data.correlationId) {
            throw new WorkflowWorldError('Step not found for step_started', {
              status: 404,
            });
          }

          if (
            validatedStep.retryAfter &&
            validatedStep.retryAfter.getTime() > Date.now()
          ) {
            const err = new WorkflowWorldError(
              `Cannot start step '${data.correlationId}' before retryAfter`,
              { status: 425 }
            );
            (err as WorkflowWorldError & { meta?: Record<string, string> }).meta = {
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
            throw new WorkflowWorldError('Step not found for step_completed', {
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
            throw new WorkflowWorldError('Step not found for step_failed', {
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
            throw new WorkflowWorldError('Step not found for step_retrying', {
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
            const recovered = await resolveHookCreateConflict(
              existingByToken,
              effectiveRunId,
              data.correlationId
            );
            if (!recovered) {
              const conflictEvent: Event = {
                eventType: 'hook_conflict',
                correlationId: data.correlationId,
                eventData: {
                  token: data.eventData.token,
                  conflictingRunId: existingByToken.runId,
                },
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
            // Orphaned hook row (crash between the hook write and the
            // hook_created event write below): reuse it and fall through to
            // complete the write, instead of re-inserting the hook.
            hook = recovered;
          } else {
            const existingById = await getHookById(data.correlationId);
            if (existingById) {
              const recovered = await resolveHookCreateConflict(
                existingById,
                effectiveRunId,
                data.correlationId
              );
              if (!recovered) {
                throw new WorkflowWorldError(
                  `Hook '${data.correlationId}' already exists`,
                  { status: 409 }
                );
              }
              hook = recovered;
            } else {
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
                isWebhook: data.eventData.isWebhook,
              } as Hook;
              try {
                await putHook(hook);
              } catch (err) {
                const conflictKind = hookConflictKind(err);
                if (conflictKind === 'token') {
                  // The transaction lost the token race after our pre-check missed
                  // it (GSI2 lag) — read the reservation item directly (strongly
                  // consistent) to find who actually won it.
                  const reservation = await getHookTokenReservation(data.eventData.token);
                  const recovered = reservation
                    ? await resolveHookCreateConflict(reservation, effectiveRunId, data.correlationId)
                    : null;
                  if (recovered) {
                    hook = recovered;
                  } else {
                    const conflictEvent: Event = {
                      eventType: 'hook_conflict',
                      correlationId: data.correlationId,
                      eventData: {
                        token: data.eventData.token,
                        conflictingRunId: reservation?.runId,
                      },
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
                } else if (conflictKind === 'hookId') {
                  // Lost the hookId guard to a concurrent creator (the pre-check
                  // above raced too) — same recovery: idempotent replay throws,
                  // an orphaned row is reused.
                  const concurrent = await getHookById(data.correlationId);
                  const recovered = concurrent
                    ? await resolveHookCreateConflict(concurrent, effectiveRunId, data.correlationId)
                    : null;
                  if (!recovered) {
                    throw new WorkflowWorldError(
                      `Hook '${data.correlationId}' already exists`,
                      { status: 409 }
                    );
                  }
                  hook = recovered;
                } else {
                  throw err;
                }
              }
            }
          }
        } else if (data.eventType === 'hook_disposed') {
          if (data.correlationId && hookForDisposal) {
            await deleteHook(effectiveRunId, data.correlationId, hookForDisposal.token);
          }
        }

        const event: Event = {
          ...data,
          runId: effectiveRunId,
          eventId,
          createdAt: now,
          specVersion: effectiveSpecVersion,
        } as Event;

        // run_started's eventData (used only transiently for the resilient-start
        // bootstrap path) is never persisted or returned — the durable copy lives
        // on run_created only, matching @workflow/world-postgres.
        if (event.eventType === 'run_started') {
          delete (event as Event & { eventData?: unknown }).eventData;
        }

        await putEvent(event);

        return {
          event: filterEventData(event, resolveData),
          run: run ? deepClone(run) : undefined,
          step: step ? deepClone(step) : undefined,
          hook: hook ? deepClone(hook) : undefined,
        };
      },

      async get(runId: string, eventId: string, params?: GetEventParams) {
        const res = await ddb.send(
          new GetCommand({
            TableName: tableName,
            Key: { PK: runPK(runId), SK: eventSK(eventId) },
          })
        );
        const event = readEvent(res.Item);
        if (!event) {
          throw new WorkflowWorldError(`Event not found: ${eventId}`, {
            status: 404,
          });
        }
        return filterEventData(event, params?.resolveData);
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
          .map((item) => readEvent(item))
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
          .map((item) => readEvent(item))
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
          throw new HookNotFoundError(hookId);
        }
        return filterHookData(hook, params?.resolveData);
      },

      async getByToken(token: string, params?: GetHookParams) {
        const hook = await getHookByToken(token);
        if (!hook) {
          throw new HookNotFoundError(token);
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
          .map((item) => readHook(item))
          .filter((h): h is Hook => h !== null);

        const page = paginate(
          hooks,
          (h) => h.hookId,
          {
            sortOrder: params.pagination?.sortOrder,
            limit: params.pagination?.limit,
            cursor: params.pagination?.cursor,
          },
          // Default to creation order (oldest first), matching
          // @workflow/world-postgres. Callers that create several hooks
          // upfront and correlate them by list position (as the webhookWorkflow
          // e2e test does) depend on this.
          'asc',
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
