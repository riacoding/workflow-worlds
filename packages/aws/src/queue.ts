/**
 * SQS Queue Implementation
 *
 * - Dispatch of step/workflow execution work via a standard SQS queue.
 * - TTL-based idempotency using a DynamoDB record (5s dedupe window) to catch
 *   network retries only — explicitly NOT inflight tracking, which deadlocks
 *   workflows (each step has a unique idempotency key, so workflow duration has
 *   no effect on idempotency).
 * - A background worker long-polls SQS and makes the HTTP callback to the
 *   workflow server. Retries use SQS visibility timeout (short backoff) or, for
 *   delays beyond SQS's limit, an EventBridge Scheduler one-off schedule.
 */

import { decode, encode } from 'cbor-x'
import type { Queue, MessageId, ValidQueueName, QueuePrefix } from '@workflow/world'
import { GetCommand, PutCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb'
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs'
import { CreateScheduleCommand, type SchedulerClient } from '@aws-sdk/client-scheduler'
import { monotonicFactory } from 'ulid'
import { z } from 'zod'
import { debug } from './utils.js'

const generateUlid = monotonicFactory()

// TTL dedupe window: long enough to catch network retries (sub-second),
// short enough to never interfere with legitimate continuations.
const IDEMPOTENCY_TTL_MS = 5000
// SQS ChangeMessageVisibility ceiling (12 hours). Longer delays go to Scheduler.
const MAX_SQS_DELAY_SECONDS = 43_200
const DEFAULT_MAX_RETRIES = 5
// Short visibility window so a message orphaned by a crashed/killed worker is
// redelivered quickly. A heartbeat extends it while a message is actively being
// processed, so legitimately slow work is not redelivered early.
const VISIBILITY_TIMEOUT_S = 6
const HEARTBEAT_MS = 3000

export interface QueueConfig {
  /** DynamoDB document client (idempotency records). */
  ddb: DynamoDBDocumentClient
  /** SQS client. */
  sqs: SQSClient
  /** EventBridge Scheduler client (long delays). */
  scheduler: SchedulerClient
  /** DynamoDB single-table name (idempotency records). */
  tableName: string
  /** Resolved SQS queue URL. */
  queueUrl: string

  /** Base URL for HTTP callbacks. Env: WORKFLOW_SERVICE_URL. Default http://localhost:{PORT}. */
  baseUrl?: string
  /** Max concurrent message processing. Env: WORKFLOW_CONCURRENCY. Default 20. */
  concurrency?: number
  /** Max delivery attempts before dropping a message. Default 5. */
  maxRetries?: number

  /** Scheduler group for long-delay schedules. */
  schedulerGroupName?: string
  /** IAM role ARN the scheduler assumes to deliver to SQS. */
  schedulerRoleArn?: string
  /** ARN of the SQS queue (scheduler target). */
  queueArn?: string
}

interface QueueEnvelope {
  queueName: ValidQueueName
  messageId: MessageId
  /** base64-encoded serialized payload. */
  payload: string
}

function getBaseUrl(configBaseUrl?: string): string {
  if (configBaseUrl) return configBaseUrl
  if (process.env.WORKFLOW_SERVICE_URL) return process.env.WORKFLOW_SERVICE_URL
  const port = process.env.PORT ?? '3000'
  return `http://localhost:${port}`
}

export function createQueue(config: QueueConfig): {
  queue: Queue
  start: () => Promise<void>
  close: () => Promise<void>
} {
  const { ddb, sqs, scheduler, tableName, queueUrl } = config
  const maxRetries = config.maxRetries ?? DEFAULT_MAX_RETRIES

  const maxConcurrency =
    config.concurrency ?? (process.env.WORKFLOW_CONCURRENCY ? parseInt(process.env.WORKFLOW_CONCURRENCY, 10) : 20)

  let currentConcurrency = 0
  const waitQueue: Array<() => void> = []

  async function acquireConcurrency(): Promise<void> {
    if (currentConcurrency < maxConcurrency) {
      currentConcurrency++
      return
    }
    return new Promise((resolve) => waitQueue.push(resolve))
  }

  function releaseConcurrency(): void {
    const next = waitQueue.shift()
    if (next) next()
    else currentConcurrency--
  }

  let running = false
  let shuttingDown = false
  let loopPromise: Promise<void> | null = null
  const inflight = new Set<Promise<void>>()
  // messageId -> receiptHandle for messages currently being processed, so a
  // graceful shutdown can release them (visibility 0) for immediate redelivery.
  const inflightHandles = new Map<string, string>()
  let signalHandlersRegistered = false

  // ---------------------------------------------------------------------------
  // Idempotency (DynamoDB conditional write with a short TTL window)
  // ---------------------------------------------------------------------------

  async function reserveIdempotencyKey(
    key: string,
    messageId: MessageId,
  ): Promise<{ duplicate: false } | { duplicate: true; messageId: MessageId }> {
    const now = Date.now()
    const pk = `IDEMPOTENCY#${key}`
    try {
      await ddb.send(
        new PutCommand({
          TableName: tableName,
          Item: {
            PK: pk,
            SK: pk,
            entity: 'idempotency',
            messageId,
            expiresAt: now + IDEMPOTENCY_TTL_MS,
            // DynamoDB TTL cleanup (epoch seconds); coarse, just for GC.
            ttl: Math.floor((now + 60_000) / 1000),
          },
          // Reserve if no live record exists (absent, or expired past the window).
          ConditionExpression: 'attribute_not_exists(PK) OR expiresAt < :now',
          ExpressionAttributeValues: { ':now': now },
        }),
      )
      return { duplicate: false }
    } catch (err) {
      if (
        typeof err === 'object' &&
        err !== null &&
        (err as { name?: string }).name === 'ConditionalCheckFailedException'
      ) {
        const existing = await ddb.send(new GetCommand({ TableName: tableName, Key: { PK: pk, SK: pk } }))
        const existingId = existing.Item?.messageId as MessageId | undefined
        return { duplicate: true, messageId: existingId ?? messageId }
      }
      throw err
    }
  }

  // ---------------------------------------------------------------------------
  // Long-delay scheduling via EventBridge Scheduler
  // ---------------------------------------------------------------------------

  async function scheduleReenqueue(envelope: QueueEnvelope, atMs: number): Promise<boolean> {
    if (!config.queueArn || !config.schedulerRoleArn) {
      debug('Long delay requested but scheduler role/queue ARN not configured; skipping')
      return false
    }
    try {
      const at = new Date(atMs)
      // Scheduler expects at(yyyy-mm-ddThh:mm:ss) with no milliseconds/zone.
      const atExpr = `at(${at.toISOString().split('.')[0]})`
      await scheduler.send(
        new CreateScheduleCommand({
          Name: `wkf-${envelope.messageId}`,
          GroupName: config.schedulerGroupName,
          ScheduleExpression: atExpr,
          FlexibleTimeWindow: { Mode: 'OFF' },
          ActionAfterCompletion: 'DELETE',
          Target: {
            Arn: config.queueArn,
            RoleArn: config.schedulerRoleArn,
            Input: JSON.stringify(envelope),
          },
        }),
      )
      return true
    } catch (err) {
      debug('scheduleReenqueue failed (non-fatal):', String(err))
      return false
    }
  }

  // ---------------------------------------------------------------------------
  // Message processing
  // ---------------------------------------------------------------------------

  async function processMessage(envelope: QueueEnvelope, receiptHandle: string, attempt: number): Promise<void> {
    const pathname = envelope.queueName.startsWith('__wkf_step_') ? 'step' : 'flow'
    const body = Buffer.from(envelope.payload, 'base64')
    debug('Worker decoded bytes:', body.length, 'first16:', Array.from(body.subarray(0, 16)))

    inflightHandles.set(envelope.messageId, receiptHandle)
    // Keep the message invisible while we actively work on it. If this worker
    // crashes or is killed, the heartbeat stops and the message reappears after
    // VISIBILITY_TIMEOUT_S instead of being stuck for a long visibility window.
    const heartbeat = setInterval(() => {
      sqs
        .send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: queueUrl,
            ReceiptHandle: receiptHandle,
            VisibilityTimeout: VISIBILITY_TIMEOUT_S,
          }),
        )
        .catch(() => {})
    }, HEARTBEAT_MS)

    const stopHeartbeat = () => clearInterval(heartbeat)

    try {
      const response = await fetch(`${getBaseUrl(config.baseUrl)}/.well-known/workflow/v1/${pathname}`, {
        method: 'POST',
        headers: {
          'content-type': 'application/cbor',
          'x-vqs-queue-name': envelope.queueName,
          'x-vqs-message-id': envelope.messageId,
          'x-vqs-message-attempt': String(attempt),
        },
        body,
      })
      stopHeartbeat()

      if (response.ok) {
        await sqs.send(
          new DeleteMessageCommand({
            QueueUrl: queueUrl,
            ReceiptHandle: receiptHandle,
          }),
        )
        return
      }

      const text = await response.text()

      // 503 { timeoutSeconds } is a scheduled retry, not a failure.
      if (response.status === 503) {
        try {
          const { timeoutSeconds } = JSON.parse(text)
          if (typeof timeoutSeconds === 'number') {
            await deferMessage(envelope, receiptHandle, timeoutSeconds)
            return
          }
        } catch (err) {
          // fall through to failure handling
          debug('deferMessage failed, falling back to failure handling:', String(err))
        }
      }

      debug('Message processing failed:', {
        messageId: envelope.messageId,
        queueName: envelope.queueName,
        status: response.status,
        attempt,
      })
      await handleFailure(envelope, receiptHandle, attempt)
    } catch (err) {
      stopHeartbeat()
      debug('Network error processing message:', String(err))
      await handleFailure(envelope, receiptHandle, attempt)
    } finally {
      stopHeartbeat()
      inflightHandles.delete(envelope.messageId)
    }
  }

  /** Defer redelivery of a message by N seconds (SQS visibility or Scheduler). */
  async function deferMessage(envelope: QueueEnvelope, receiptHandle: string, delaySeconds: number): Promise<void> {
    if (delaySeconds <= MAX_SQS_DELAY_SECONDS) {
      await sqs.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: receiptHandle,
          VisibilityTimeout: Math.max(0, Math.ceil(delaySeconds)),
        }),
      )
      return
    }
    // Beyond SQS's window: hand off to EventBridge Scheduler and drop the
    // in-flight SQS message so it is not redelivered early.
    const scheduled = await scheduleReenqueue(envelope, Date.now() + delaySeconds * 1000)
    if (scheduled) {
      await sqs.send(
        new DeleteMessageCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: receiptHandle,
        }),
      )
    } else {
      // Could not schedule; keep it in SQS at the max visibility delay.
      await sqs.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: receiptHandle,
          VisibilityTimeout: MAX_SQS_DELAY_SECONDS,
        }),
      )
    }
  }

  async function handleFailure(envelope: QueueEnvelope, receiptHandle: string, attempt: number): Promise<void> {
    if (attempt >= maxRetries) {
      // Exhausted retries: drop the message (a DLQ redrive policy would catch
      // this in production).
      debug('Max retries reached, dropping message:', envelope.messageId)
      await sqs.send(
        new DeleteMessageCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: receiptHandle,
        }),
      )
      return
    }
    // Exponential backoff (1s, 2s, 4s, ...) via SQS visibility timeout.
    const backoff = Math.min(MAX_SQS_DELAY_SECONDS, Math.pow(2, attempt - 1))
    await sqs.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
        VisibilityTimeout: backoff,
      }),
    )
  }

  // ---------------------------------------------------------------------------
  // Worker loop
  // ---------------------------------------------------------------------------

  async function pollLoop(): Promise<void> {
    while (running && !shuttingDown) {
      let received
      try {
        received = await sqs.send(
          new ReceiveMessageCommand({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: 10,
            WaitTimeSeconds: 5,
            VisibilityTimeout: VISIBILITY_TIMEOUT_S,
            MessageAttributeNames: ['All'],
            MessageSystemAttributeNames: ['ApproximateReceiveCount'],
          }),
        )
      } catch (err) {
        if (!shuttingDown) debug('ReceiveMessage error:', String(err))
        await new Promise((r) => setTimeout(r, 250))
        continue
      }

      const messages = received.Messages ?? []
      for (const msg of messages) {
        if (!msg.Body || !msg.ReceiptHandle) continue

        let envelope: QueueEnvelope
        try {
          envelope = JSON.parse(msg.Body) as QueueEnvelope
        } catch (err) {
          debug('Malformed message body, deleting:', String(err))
          await sqs.send(
            new DeleteMessageCommand({
              QueueUrl: queueUrl,
              ReceiptHandle: msg.ReceiptHandle,
            }),
          )
          continue
        }

        const attempt = Number(msg.Attributes?.ApproximateReceiveCount ?? '1')

        await acquireConcurrency()
        const receiptHandle = msg.ReceiptHandle
        const task = processMessage(envelope, receiptHandle, attempt)
          .catch((err) => debug('processMessage error:', String(err)))
          .finally(() => releaseConcurrency())
        inflight.add(task)
        task.finally(() => inflight.delete(task))
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Queue interface
  // ---------------------------------------------------------------------------

  const queue: Queue = {
    async getDeploymentId(): Promise<string> {
      return process.env.DEPLOYMENT_ID ?? 'dpl_aws'
    },

    async queue(
      queueName: ValidQueueName,
      message: unknown,
      opts?: { deploymentId?: string; idempotencyKey?: string },
    ): Promise<{ messageId: MessageId }> {
      if (!queueName.startsWith('__wkf_step_') && !queueName.startsWith('__wkf_workflow_')) {
        throw new Error(`Unknown queue prefix in: ${queueName}`)
      }

      const messageId = `msg_${generateUlid()}` as MessageId

      if (opts?.idempotencyKey) {
        const reservation = await reserveIdempotencyKey(opts.idempotencyKey, messageId)
        if (reservation.duplicate) {
          return { messageId: reservation.messageId }
        }
      }

      // CBOR (not JSON) so binary fields survive the trip — queueMessage's
      // runInput.input is devalue-encoded Uint8Array, and JSON.stringify would
      // silently mangle it into a plain {"0":1,"1":2,...} object. This world
      // declares specVersion 3 (SPEC_VERSION_SUPPORTS_CBOR_QUEUE_TRANSPORT),
      // which is exactly the contract that requires this.
      const serialized = encode(message)
      debug(
        'Sending bytes:',
        serialized.length,
        'first16:',
        Array.from(serialized.slice(0, 16)),
      )
      const envelope: QueueEnvelope = {
        queueName,
        messageId,
        payload: Buffer.from(serialized).toString('base64'),
      }

      await sqs.send(
        new SendMessageCommand({
          QueueUrl: queueUrl,
          MessageBody: JSON.stringify(envelope),
          MessageAttributes: {
            queueName: { DataType: 'String', StringValue: queueName },
            messageId: { DataType: 'String', StringValue: messageId },
          },
        }),
      )

      return { messageId }
    },

    createQueueHandler(
      prefix: QueuePrefix,
      handler: (
        message: unknown,
        meta: {
          attempt: number
          queueName: ValidQueueName
          messageId: MessageId
        },
      ) => Promise<void | { timeoutSeconds: number }>,
    ): (req: Request) => Promise<Response> {
      const HeaderParser = z.object({
        'x-vqs-queue-name': z.string(),
        'x-vqs-message-id': z.string(),
        'x-vqs-message-attempt': z.coerce.number(),
      })

      return async (req: Request): Promise<Response> => {
        const headers = HeaderParser.safeParse(Object.fromEntries(req.headers))

        if (!headers.success || !req.body) {
          return Response.json(
            {
              error: !req.body ? 'Missing request body' : 'Missing required headers',
            },
            { status: 400 },
          )
        }

        const queueName = headers.data['x-vqs-queue-name'] as ValidQueueName
        const messageId = headers.data['x-vqs-message-id'] as MessageId
        const attempt = headers.data['x-vqs-message-attempt']

        if (!queueName.startsWith(prefix)) {
          return Response.json({ error: 'Unhandled queue' }, { status: 400 })
        }

        const bodyBytes = new Uint8Array(await req.arrayBuffer())
        debug(
          'Received bytes:',
          bodyBytes.length,
          'first16:',
          Array.from(bodyBytes.slice(0, 16)),
        )
        const body = decode(bodyBytes)

        try {
          const result = await handler(body, { attempt, queueName, messageId })
          // `timeoutSeconds: 0` means "re-invoke immediately" (e.g. hook conflict
          // reporting, hook.getConflict() continuations) and must still take the
          // 503 branch — a truthy check would drop it, since 0 is falsy in JS.
          if (result?.timeoutSeconds !== undefined) {
            return Response.json({ timeoutSeconds: result.timeoutSeconds }, { status: 503 })
          }
          return Response.json({ ok: true })
        } catch (error) {
          debug('Handler error:', error)
          return Response.json(String(error), { status: 500 })
        }
      }
    },
  }

  async function releaseInflight(): Promise<void> {
    // Make in-flight messages immediately visible again so another worker can
    // pick them up without waiting out the visibility timeout.
    await Promise.allSettled(
      [...inflightHandles.values()].map((receiptHandle) =>
        sqs.send(
          new ChangeMessageVisibilityCommand({
            QueueUrl: queueUrl,
            ReceiptHandle: receiptHandle,
            VisibilityTimeout: 0,
          }),
        ),
      ),
    )
  }

  async function start(): Promise<void> {
    if (running) return
    running = true
    shuttingDown = false
    debug('Starting SQS worker on queue:', queueUrl)
    loopPromise = pollLoop()

    if (!signalHandlersRegistered) {
      signalHandlersRegistered = true
      const onSignal = () => {
        void (async () => {
          try {
            await close()
          } finally {
            process.exit(0)
          }
        })()
      }
      process.once('SIGTERM', onSignal)
      process.once('SIGINT', onSignal)
    }
  }

  async function close(): Promise<void> {
    if (!running) return
    shuttingDown = true
    running = false
    await releaseInflight()
    await loopPromise?.catch(() => {})
    await Promise.allSettled([...inflight])
    debug('SQS worker stopped')
  }

  return { queue, start, close }
}
