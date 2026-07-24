/**
 * Domain types for the business-process state store.
 *
 * A "state record" is the current position of some subject (e.g. a user)
 * within some business process (e.g. "onboarding"). A subject may be in
 * several processes at once, each tracked independently.
 */

export interface StateRecord {
  applicationId: string;
  subjectType: string;
  subjectId: string;
  process: string;
  state: string;
  status?: string;
  runId?: string;
  metadata?: Record<string, unknown>;
  createdAt: string;
  updatedAt: string;
}

export interface PutStateInput {
  applicationId: string;
  subjectType: string;
  subjectId: string;
  process: string;
  state: string;
  status?: string;
  runId?: string;
  metadata?: Record<string, unknown>;
}
