/**
 * Streamer Implementation (DynamoDB persistence + AppSync Events pub/sub)
 *
 * Stream chunks are persisted in the same DynamoDB single table so that
 * `readFromStream()` can replay history and resume from an index. Real-time
 * delivery uses an in-process EventEmitter (covers the common single-process
 * case) plus a lightweight DynamoDB tail poll (covers cross-process readers).
 * When an AppSync Events API endpoint is configured, each chunk is also
 * published to a per-stream channel for fan-out to external subscribers.
 *
 * Chunk ordering is guaranteed by monotonic ULID chunk ids used as the sort
 * key suffix (CHUNK#<ulid>).
 */

import { EventEmitter } from 'node:events';
import type {
  GetChunksOptions,
  Streamer,
  StreamChunksResponse,
  StreamInfoResponse,
} from '@workflow/world';
import {
  PutCommand,
  QueryCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import { monotonicFactory } from 'ulid';
import { debug } from './utils.js';

const generateUlid = monotonicFactory();

interface StreamChunk {
  chunkId: string;
  streamName: string;
  data: Uint8Array;
  eof: boolean;
}

const TAIL_POLL_INTERVAL_MS = 150;

export interface DynamoStreamerConfig {
  ddb: DynamoDBDocumentClient;
  tableName: string;
  /** AppSync Events API HTTP endpoint (optional; enables external fan-out). */
  appsyncEventsEndpoint?: string;
  /** AppSync Events API key (x-api-key auth). */
  appsyncApiKey?: string;
  /**
   * Enable the DynamoDB tail poll used for cross-process realtime delivery.
   * Default true. Tests run single-process so the emitter alone suffices, but
   * the poll makes multi-process reads correct.
   */
  enableTailPoll?: boolean;
}

const streamPK = (name: string) => `STREAM#${name}`;
const chunkSK = (chunkId: string) => `CHUNK#${chunkId}`;
const streamRunPK = (runId: string) => `STREAMRUN#${runId}`;

function toUint8Array(data: unknown): Uint8Array {
  if (data instanceof Uint8Array) return data;
  if (typeof data === 'string') {
    // Stored as base64.
    return new Uint8Array(Buffer.from(data, 'base64'));
  }
  return new Uint8Array(0);
}

export function createStreamer(config: DynamoStreamerConfig): Streamer {
  const { ddb, tableName } = config;
  const enableTailPoll = config.enableTailPoll ?? true;

  const emitter = new EventEmitter<{
    [key: `chunk:${string}`]: [StreamChunk];
    [key: `close:${string}`]: [];
  }>();
  emitter.setMaxListeners(1000);

  async function persistChunk(chunk: StreamChunk): Promise<void> {
    await ddb.send(
      new PutCommand({
        TableName: tableName,
        Item: {
          PK: streamPK(chunk.streamName),
          SK: chunkSK(chunk.chunkId),
          entity: 'chunk',
          streamName: chunk.streamName,
          chunkId: chunk.chunkId,
          eof: chunk.eof,
          data: Buffer.from(chunk.data).toString('base64'),
        },
      })
    );
  }

  async function registerStream(runId: string, name: string): Promise<void> {
    await ddb.send(
      new PutCommand({
        TableName: tableName,
        Item: {
          PK: streamRunPK(runId),
          SK: streamPK(name),
          entity: 'streamrun',
          runId,
          streamName: name,
        },
      })
    );
  }

  async function loadChunks(
    name: string,
    afterChunkId?: string
  ): Promise<StreamChunk[]> {
    const items: Record<string, unknown>[] = [];
    let startKey: Record<string, unknown> | undefined;
    do {
      const res = await ddb.send(
        new QueryCommand({
          TableName: tableName,
          KeyConditionExpression: afterChunkId
            ? 'PK = :pk AND SK > :after'
            : 'PK = :pk AND begins_with(SK, :sk)',
          ExpressionAttributeValues: afterChunkId
            ? { ':pk': streamPK(name), ':after': chunkSK(afterChunkId) }
            : { ':pk': streamPK(name), ':sk': 'CHUNK#' },
          ScanIndexForward: true,
          ExclusiveStartKey: startKey,
        })
      );
      if (res.Items) items.push(...res.Items);
      startKey = res.LastEvaluatedKey as Record<string, unknown> | undefined;
    } while (startKey);

    return items.map((item) => ({
      chunkId: item.chunkId as string,
      streamName: name,
      data: toUint8Array(item.data),
      eof: Boolean(item.eof),
    }));
  }

  async function publishToAppsync(name: string, chunk: StreamChunk): Promise<void> {
    if (!config.appsyncEventsEndpoint) return;
    try {
      const headers: Record<string, string> = {
        'content-type': 'application/json',
      };
      if (config.appsyncApiKey) headers['x-api-key'] = config.appsyncApiKey;
      await fetch(config.appsyncEventsEndpoint, {
        method: 'POST',
        headers,
        body: JSON.stringify({
          channel: `/streams/${name}`,
          events: [
            JSON.stringify({
              chunkId: chunk.chunkId,
              eof: chunk.eof,
              data: Buffer.from(chunk.data).toString('base64'),
            }),
          ],
        }),
      });
    } catch (err) {
      debug('AppSync Events publish failed (non-fatal):', String(err));
    }
  }

  return {
    async writeToStream(
      name: string,
      runId: string,
      chunk: string | Uint8Array
    ): Promise<void> {
      await registerStream(runId, name);

      const data =
        typeof chunk === 'string' ? new TextEncoder().encode(chunk) : chunk;
      const streamChunk: StreamChunk = {
        chunkId: `chnk_${generateUlid()}`,
        streamName: name,
        data,
        eof: false,
      };

      await persistChunk(streamChunk);
      emitter.emit(`chunk:${name}`, streamChunk);
      await publishToAppsync(name, streamChunk);
    },

    async closeStream(
      name: string,
      runId: string
    ): Promise<void> {
      await registerStream(runId, name);

      const streamChunk: StreamChunk = {
        chunkId: `chnk_${generateUlid()}`,
        streamName: name,
        data: new Uint8Array(0),
        eof: true,
      };

      await persistChunk(streamChunk);
      emitter.emit(`close:${name}`);
      await publishToAppsync(name, streamChunk);
    },

    async getStreamChunks(
      name: string,
      _runId: string,
      options?: GetChunksOptions
    ): Promise<StreamChunksResponse> {
      const all = await loadChunks(name);
      const dataChunks = all.filter((c) => !c.eof);
      const done = all.some((c) => c.eof);

      const limit = options?.limit ?? 100;
      let startIdx = 0;
      if (options?.cursor) {
        const cursorIdx = Number(options.cursor);
        startIdx = Number.isFinite(cursorIdx) ? cursorIdx + 1 : 0;
      }

      const windowed = dataChunks.slice(startIdx, startIdx + limit);
      const hasMore = startIdx + limit < dataChunks.length;

      return {
        data: windowed.map((chunk, i) => ({
          index: startIdx + i,
          data: chunk.data,
        })),
        cursor: hasMore ? String(startIdx + windowed.length - 1) : null,
        hasMore,
        done,
      };
    },

    async getStreamInfo(name: string, _runId: string): Promise<StreamInfoResponse> {
      const all = await loadChunks(name);
      const dataChunks = all.filter((c) => !c.eof);
      const done = all.some((c) => c.eof);
      return {
        tailIndex: dataChunks.length - 1,
        done,
      };
    },

    async listStreamsByRunId(runId: string): Promise<string[]> {
      const res = await ddb.send(
        new QueryCommand({
          TableName: tableName,
          KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
          ExpressionAttributeValues: {
            ':pk': streamRunPK(runId),
            ':sk': 'STREAM#',
          },
          ScanIndexForward: true,
        })
      );
      return (res.Items ?? []).map((item) => item.streamName as string);
    },

    async readFromStream(
      name: string,
      startIndex = 0
    ): Promise<ReadableStream<Uint8Array>> {
      return new ReadableStream<Uint8Array>({
        async start(controller) {
          const deliveredChunkIds = new Set<string>();
          const bufferedEventChunks: StreamChunk[] = [];
          let isLoadingFromStorage = true;
          let closeRequested = false;
          let isClosed = false;
          let lastChunkId: string | undefined;
          let tailTimer: ReturnType<typeof setInterval> | undefined;

          const cleanup = () => {
            emitter.off(`chunk:${name}`, chunkHandler);
            emitter.off(`close:${name}`, closeHandler);
            if (tailTimer) clearInterval(tailTimer);
          };

          const closeController = () => {
            if (isClosed) return;
            isClosed = true;
            cleanup();
            try {
              controller.close();
            } catch {
              // already closed
            }
          };

          const deliver = (chunk: StreamChunk) => {
            if (deliveredChunkIds.has(chunk.chunkId)) return;
            deliveredChunkIds.add(chunk.chunkId);
            lastChunkId = chunk.chunkId;
            if (chunk.eof) {
              closeController();
              return;
            }
            if (chunk.data.byteLength > 0) {
              controller.enqueue(Uint8Array.from(chunk.data));
            }
          };

          const chunkHandler = (chunk: StreamChunk) => {
            if (isClosed) return;
            if (deliveredChunkIds.has(chunk.chunkId)) return;
            if (chunk.data.byteLength === 0 && !chunk.eof) return;
            if (isLoadingFromStorage) {
              bufferedEventChunks.push(chunk);
            } else {
              deliver(chunk);
            }
          };

          const closeHandler = () => {
            if (isLoadingFromStorage) {
              closeRequested = true;
              return;
            }
            closeController();
          };

          // Subscribe before loading so we don't miss concurrent writes.
          emitter.on(`chunk:${name}`, chunkHandler);
          emitter.on(`close:${name}`, closeHandler);

          const existing = await loadChunks(name);
          for (let i = startIndex; i < existing.length; i++) {
            const chunk = existing[i];
            lastChunkId = chunk.chunkId;
            if (chunk.eof) {
              closeController();
              return;
            }
            if (deliveredChunkIds.has(chunk.chunkId)) continue;
            deliveredChunkIds.add(chunk.chunkId);
            if (chunk.data.byteLength > 0) {
              controller.enqueue(Uint8Array.from(chunk.data));
            }
          }

          isLoadingFromStorage = false;

          bufferedEventChunks.sort((a, b) =>
            a.chunkId.localeCompare(b.chunkId)
          );
          for (const chunk of bufferedEventChunks) {
            if (isClosed) break;
            deliver(chunk);
          }

          if (closeRequested || existing[existing.length - 1]?.eof) {
            closeController();
            return;
          }

          // Tail poll picks up chunks written by other processes (which never
          // reach this process's emitter). No-op when everything comes through
          // the emitter because chunkIds are deduplicated.
          if (enableTailPoll) {
            tailTimer = setInterval(async () => {
              if (isClosed) return;
              try {
                const newChunks = await loadChunks(name, lastChunkId);
                for (const chunk of newChunks) {
                  if (isClosed) break;
                  deliver(chunk);
                }
              } catch (err) {
                debug('tail poll error (non-fatal):', String(err));
              }
            }, TAIL_POLL_INTERVAL_MS);
          }
        },

        cancel() {
          emitter.removeAllListeners(`chunk:${name}`);
          emitter.removeAllListeners(`close:${name}`);
        },
      });
    },
  };
}
