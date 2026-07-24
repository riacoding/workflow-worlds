/**
 * Lambda handler behind the REST API Gateway proxy integration.
 *
 * Routes (see infra/src/lib/business-state-stack.ts for the API Gateway
 * resource tree):
 *   PUT /state/{subjectType}/{subjectId}/{process}  — upsert state
 *   GET /state/{subjectType}/{subjectId}/{process}  — read one process
 *   GET /state/{subjectType}/{subjectId}            — list all processes
 *   GET /runs/{runId}/state                         — lookup by run id
 *
 * `applicationId` is never read from the request — it's resolved server-side
 * from the caller's API key id via resolveApplicationId().
 */

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { resolveApplicationId } from './auth.js';
import { createBusinessStateStore, type BusinessStateStore } from './store.js';
import { debug } from './utils.js';

class BusinessStateApiError extends Error {
  status: number;
  constructor(message: string, status: number) {
    super(message);
    this.status = status;
  }
}

function jsonResponse(status: number, body: unknown): APIGatewayProxyResult {
  return {
    statusCode: status,
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  };
}

function requireParam(
  params: APIGatewayProxyEvent['pathParameters'],
  name: string
): string {
  const value = params?.[name];
  if (!value) throw new BusinessStateApiError(`Missing path parameter: ${name}`, 400);
  return decodeURIComponent(value);
}

function parseBody(raw: string | null): Record<string, unknown> {
  if (!raw) throw new BusinessStateApiError('Missing request body', 400);
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    throw new BusinessStateApiError('Request body must be valid JSON', 400);
  }
  if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) {
    throw new BusinessStateApiError('Request body must be a JSON object', 400);
  }
  return parsed as Record<string, unknown>;
}

function validatePutBody(body: Record<string, unknown>) {
  if (typeof body.state !== 'string' || body.state.length === 0) {
    throw new BusinessStateApiError(
      "Field 'state' is required and must be a non-empty string",
      400
    );
  }
  if (body.status !== undefined && typeof body.status !== 'string') {
    throw new BusinessStateApiError("Field 'status' must be a string", 400);
  }
  if (body.runId !== undefined && typeof body.runId !== 'string') {
    throw new BusinessStateApiError("Field 'runId' must be a string", 400);
  }
  if (
    body.metadata !== undefined &&
    (typeof body.metadata !== 'object' || body.metadata === null || Array.isArray(body.metadata))
  ) {
    throw new BusinessStateApiError("Field 'metadata' must be an object", 400);
  }
}

async function routeRequest(
  store: BusinessStateStore,
  applicationId: string,
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> {
  const method = event.httpMethod;
  const params = event.pathParameters;

  // GET /runs/{runId}/state
  if (params?.runId !== undefined) {
    if (method !== 'GET') {
      throw new BusinessStateApiError(`Unsupported method: ${method}`, 405);
    }
    const runId = requireParam(params, 'runId');
    const items = await store.getStatesByRunId(applicationId, runId);
    if (items.length === 0) {
      throw new BusinessStateApiError(`No state found for run: ${runId}`, 404);
    }
    return jsonResponse(200, { items });
  }

  const subjectType = requireParam(params, 'subjectType');
  const subjectId = requireParam(params, 'subjectId');

  // GET /state/{subjectType}/{subjectId}
  if (params?.process === undefined) {
    if (method !== 'GET') {
      throw new BusinessStateApiError(`Unsupported method: ${method}`, 405);
    }
    const items = await store.listStatesForSubject(applicationId, subjectType, subjectId);
    return jsonResponse(200, { items });
  }

  // /state/{subjectType}/{subjectId}/{process}
  const processName = requireParam(params, 'process');

  if (method === 'GET') {
    const record = await store.getState(applicationId, subjectType, subjectId, processName);
    if (!record) throw new BusinessStateApiError('State not found', 404);
    return jsonResponse(200, record);
  }

  if (method === 'PUT') {
    const body = parseBody(event.body);
    validatePutBody(body);
    const record = await store.putState({
      applicationId,
      subjectType,
      subjectId,
      process: processName,
      state: body.state as string,
      status: body.status as string | undefined,
      runId: body.runId as string | undefined,
      metadata: body.metadata as Record<string, unknown> | undefined,
    });
    return jsonResponse(200, record);
  }

  throw new BusinessStateApiError(`Unsupported method: ${method}`, 405);
}

/** Builds a handler bound to a specific store — used directly in tests. */
export function createHandler(store: BusinessStateStore) {
  return async function handler(
    event: APIGatewayProxyEvent
  ): Promise<APIGatewayProxyResult> {
    try {
      const applicationId = resolveApplicationId(
        event.requestContext.identity.apiKeyId ?? undefined
      );
      if (!applicationId) {
        throw new BusinessStateApiError('Unrecognized API key', 403);
      }
      return await routeRequest(store, applicationId, event);
    } catch (err) {
      if (err instanceof BusinessStateApiError) {
        debug('request failed:', err.status, err.message);
        return jsonResponse(err.status, { error: err.message });
      }
      debug('unexpected error:', String(err));
      return jsonResponse(500, { error: 'Internal error' });
    }
  };
}

let cachedStore: BusinessStateStore | undefined;

function resolveDefaultStore(): BusinessStateStore {
  if (!cachedStore) {
    const tableName = process.env.BUSINESS_STATE_TABLE_NAME;
    if (!tableName) {
      throw new Error('Missing BUSINESS_STATE_TABLE_NAME environment variable');
    }
    const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}));
    cachedStore = createBusinessStateStore({ ddb, tableName });
  }
  return cachedStore;
}

/** Lambda entry point — table resolved from BUSINESS_STATE_TABLE_NAME on first invocation. */
export async function handler(
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> {
  return createHandler(resolveDefaultStore())(event);
}
