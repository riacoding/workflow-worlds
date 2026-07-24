import type { APIGatewayProxyEvent } from 'aws-lambda';
import { beforeAll, describe, expect, it } from 'vitest';
import { createHandler } from '../dist/handler.js';
import { getTestStore } from './setup.js';

process.env.BUSINESS_STATE_APP_KEY_MAP = JSON.stringify({
  'key-app-a': 'app-a',
  'key-app-b': 'app-b',
});

function makeEvent(overrides: {
  httpMethod: string;
  pathParameters: Record<string, string> | null;
  body?: string | null;
  apiKeyId?: string;
}): APIGatewayProxyEvent {
  return {
    httpMethod: overrides.httpMethod,
    pathParameters: overrides.pathParameters,
    body: overrides.body ?? null,
    resource: '',
    path: '',
    headers: {},
    multiValueHeaders: {},
    queryStringParameters: null,
    multiValueQueryStringParameters: null,
    stageVariables: null,
    isBase64Encoded: false,
    requestContext: {
      identity: { apiKeyId: overrides.apiKeyId },
    },
  } as unknown as APIGatewayProxyEvent;
}

describe('business state handler', () => {
  let handler: ReturnType<typeof createHandler>;

  beforeAll(async () => {
    const store = await getTestStore();
    handler = createHandler(store);
  });

  it('rejects requests with an unrecognized API key', async () => {
    const res = await handler(
      makeEvent({
        httpMethod: 'GET',
        pathParameters: { subjectType: 'user', subjectId: 'u1' },
        apiKeyId: 'unknown-key',
      })
    );
    expect(res.statusCode).toBe(403);
  });

  it('upserts state via PUT and reads it back via GET', async () => {
    const putRes = await handler(
      makeEvent({
        httpMethod: 'PUT',
        pathParameters: { subjectType: 'user', subjectId: 'h1', process: 'onboarding' },
        body: JSON.stringify({ state: 'CONNECT_CALENDAR', status: 'WAITING_FOR_USER' }),
        apiKeyId: 'key-app-a',
      })
    );
    expect(putRes.statusCode).toBe(200);
    const putBody = JSON.parse(putRes.body);
    expect(putBody.state).toBe('CONNECT_CALENDAR');

    const getRes = await handler(
      makeEvent({
        httpMethod: 'GET',
        pathParameters: { subjectType: 'user', subjectId: 'h1', process: 'onboarding' },
        apiKeyId: 'key-app-a',
      })
    );
    expect(getRes.statusCode).toBe(200);
    expect(JSON.parse(getRes.body).state).toBe('CONNECT_CALENDAR');
  });

  it('rejects a PUT with a missing state field', async () => {
    const res = await handler(
      makeEvent({
        httpMethod: 'PUT',
        pathParameters: { subjectType: 'user', subjectId: 'h2', process: 'onboarding' },
        body: JSON.stringify({ status: 'WAITING' }),
        apiKeyId: 'key-app-a',
      })
    );
    expect(res.statusCode).toBe(400);
  });

  it('returns 404 for a subject/process that was never written', async () => {
    const res = await handler(
      makeEvent({
        httpMethod: 'GET',
        pathParameters: { subjectType: 'user', subjectId: 'nope', process: 'onboarding' },
        apiKeyId: 'key-app-a',
      })
    );
    expect(res.statusCode).toBe(404);
  });

  it('lists all processes for a subject', async () => {
    await handler(
      makeEvent({
        httpMethod: 'PUT',
        pathParameters: { subjectType: 'user', subjectId: 'h3', process: 'onboarding' },
        body: JSON.stringify({ state: 'STEP_1' }),
        apiKeyId: 'key-app-a',
      })
    );
    await handler(
      makeEvent({
        httpMethod: 'PUT',
        pathParameters: { subjectType: 'user', subjectId: 'h3', process: 'billing_dispute' },
        body: JSON.stringify({ state: 'OPENED' }),
        apiKeyId: 'key-app-a',
      })
    );

    const res = await handler(
      makeEvent({
        httpMethod: 'GET',
        pathParameters: { subjectType: 'user', subjectId: 'h3' },
        apiKeyId: 'key-app-a',
      })
    );
    expect(res.statusCode).toBe(200);
    expect(JSON.parse(res.body).items).toHaveLength(2);
  });

  it('returns 404 (not 403) when a runId belongs to another tenant', async () => {
    await handler(
      makeEvent({
        httpMethod: 'PUT',
        pathParameters: { subjectType: 'user', subjectId: 'h4', process: 'onboarding' },
        body: JSON.stringify({ state: 'STEP_1', runId: 'wrun_handler_test' }),
        apiKeyId: 'key-app-a',
      })
    );

    const res = await handler(
      makeEvent({
        httpMethod: 'GET',
        pathParameters: { runId: 'wrun_handler_test' },
        apiKeyId: 'key-app-b',
      })
    );
    expect(res.statusCode).toBe(404);

    const ownerRes = await handler(
      makeEvent({
        httpMethod: 'GET',
        pathParameters: { runId: 'wrun_handler_test' },
        apiKeyId: 'key-app-a',
      })
    );
    expect(ownerRes.statusCode).toBe(200);
  });
});
