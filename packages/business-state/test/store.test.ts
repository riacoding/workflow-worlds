import { describe, expect, it } from 'vitest';
import { getTestStore } from './setup.js';

describe('business state store', () => {
  it('creates a record on first write and preserves createdAt on update', async () => {
    const store = await getTestStore();
    const applicationId = 'app-a';
    const subjectType = 'user';
    const subjectId = 'u1';
    const process = 'onboarding';

    const created = await store.putState({
      applicationId,
      subjectType,
      subjectId,
      process,
      state: 'CONNECT_CALENDAR',
      status: 'WAITING_FOR_USER',
    });
    expect(created.createdAt).toBe(created.updatedAt);

    await new Promise((r) => setTimeout(r, 5));

    const updated = await store.putState({
      applicationId,
      subjectType,
      subjectId,
      process,
      state: 'COMPLETE',
    });
    expect(updated.createdAt).toBe(created.createdAt);
    expect(updated.updatedAt).not.toBe(created.updatedAt);
    // status omitted on the second write — full-replace PUT semantics clear it.
    expect(updated.status).toBeUndefined();
  });

  it('supports multiple concurrent processes per subject', async () => {
    const store = await getTestStore();
    const applicationId = 'app-a';
    const subjectType = 'user';
    const subjectId = 'u2';

    await store.putState({
      applicationId,
      subjectType,
      subjectId,
      process: 'onboarding',
      state: 'STEP_1',
    });
    await store.putState({
      applicationId,
      subjectType,
      subjectId,
      process: 'billing_dispute',
      state: 'OPENED',
    });

    const items = await store.listStatesForSubject(applicationId, subjectType, subjectId);
    expect(items).toHaveLength(2);
    expect(items.map((i) => i.process).sort()).toEqual(['billing_dispute', 'onboarding']);
  });

  it('populates GSI1 only when runId is supplied, and clears it when omitted later', async () => {
    const store = await getTestStore();
    const applicationId = 'app-a';
    const subjectType = 'user';
    const subjectId = 'u3';
    const process = 'onboarding';
    const runId = 'wrun_test123';

    await store.putState({
      applicationId,
      subjectType,
      subjectId,
      process,
      state: 'STEP_1',
      runId,
    });

    const byRun = await store.getStatesByRunId(applicationId, runId);
    expect(byRun).toHaveLength(1);
    expect(byRun[0]?.subjectId).toBe(subjectId);

    // Omitting runId on a later write clears the GSI entry.
    await store.putState({
      applicationId,
      subjectType,
      subjectId,
      process,
      state: 'STEP_2',
    });

    const byRunAfter = await store.getStatesByRunId(applicationId, runId);
    expect(byRunAfter).toHaveLength(0);
  });

  it('does not leak another tenant\'s state across a shared runId', async () => {
    const store = await getTestStore();
    const runId = 'wrun_shared';

    await store.putState({
      applicationId: 'app-a',
      subjectType: 'user',
      subjectId: 'u4',
      process: 'onboarding',
      state: 'STEP_1',
      runId,
    });

    const forOtherApp = await store.getStatesByRunId('app-b', runId);
    expect(forOtherApp).toHaveLength(0);

    const forOwner = await store.getStatesByRunId('app-a', runId);
    expect(forOwner).toHaveLength(1);
  });

  it('returns null for a subject/process that has never been written', async () => {
    const store = await getTestStore();
    const record = await store.getState('app-a', 'user', 'does-not-exist', 'onboarding');
    expect(record).toBeNull();
  });
});
