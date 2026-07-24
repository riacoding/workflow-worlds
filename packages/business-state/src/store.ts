/**
 * DynamoDB access for the business-process state store.
 *
 * Single table, tenancy baked into the partition key itself (never trusted
 * from request input): PK = APP#<applicationId>#SUB#<subjectType>#<subjectId>,
 * SK = PROC#<process>. A sparse GSI1 (PK = RUN#<runId>, SK = PROC#<process>,
 * written only when runId is supplied) supports "find state by run id"
 * without a table scan.
 */

import {
  GetCommand,
  QueryCommand,
  UpdateCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import type { PutStateInput, StateRecord } from './types.js';

export interface BusinessStateStoreConfig {
  ddb: DynamoDBDocumentClient;
  tableName: string;
}

export interface BusinessStateStore {
  putState(input: PutStateInput): Promise<StateRecord>;
  getState(
    applicationId: string,
    subjectType: string,
    subjectId: string,
    process: string
  ): Promise<StateRecord | null>;
  listStatesForSubject(
    applicationId: string,
    subjectType: string,
    subjectId: string
  ): Promise<StateRecord[]>;
  getStatesByRunId(
    applicationId: string,
    runId: string
  ): Promise<StateRecord[]>;
}

const pk = (applicationId: string, subjectType: string, subjectId: string) =>
  `APP#${applicationId}#SUB#${subjectType}#${subjectId}`;
const sk = (process: string) => `PROC#${process}`;
const gsi1pk = (runId: string) => `RUN#${runId}`;
const gsi1sk = (process: string) => `PROC#${process}`;

function toStateRecord(item: Record<string, unknown>): StateRecord {
  return {
    applicationId: item.applicationId as string,
    subjectType: item.subjectType as string,
    subjectId: item.subjectId as string,
    process: item.process as string,
    state: item.state as string,
    status: item.status as string | undefined,
    runId: item.runId as string | undefined,
    metadata: item.metadata as Record<string, unknown> | undefined,
    createdAt: item.createdAt as string,
    updatedAt: item.updatedAt as string,
  };
}

export function createBusinessStateStore(
  config: BusinessStateStoreConfig
): BusinessStateStore {
  const { ddb, tableName } = config;

  return {
    async putState(input) {
      const now = new Date().toISOString();

      const setParts = [
        'applicationId = :applicationId',
        'subjectType = :subjectType',
        'subjectId = :subjectId',
        '#process = :process',
        '#state = :state',
        'updatedAt = :now',
        'createdAt = if_not_exists(createdAt, :now)',
      ];
      const removeParts: string[] = [];
      const names: Record<string, string> = {
        '#process': 'process',
        '#state': 'state',
      };
      const values: Record<string, unknown> = {
        ':applicationId': input.applicationId,
        ':subjectType': input.subjectType,
        ':subjectId': input.subjectId,
        ':process': input.process,
        ':state': input.state,
        ':now': now,
      };

      if (input.status !== undefined) {
        setParts.push('#status = :status');
        names['#status'] = 'status';
        values[':status'] = input.status;
      } else {
        removeParts.push('#status');
        names['#status'] = 'status';
      }

      if (input.metadata !== undefined) {
        setParts.push('metadata = :metadata');
        values[':metadata'] = input.metadata;
      } else {
        removeParts.push('metadata');
      }

      if (input.runId !== undefined) {
        setParts.push('runId = :runId', 'GSI1PK = :gsi1pk', 'GSI1SK = :gsi1sk');
        values[':runId'] = input.runId;
        values[':gsi1pk'] = gsi1pk(input.runId);
        values[':gsi1sk'] = gsi1sk(input.process);
      } else {
        removeParts.push('runId', 'GSI1PK', 'GSI1SK');
      }

      const updateExpression =
        `SET ${setParts.join(', ')}` +
        (removeParts.length > 0 ? ` REMOVE ${removeParts.join(', ')}` : '');

      const res = await ddb.send(
        new UpdateCommand({
          TableName: tableName,
          Key: {
            PK: pk(input.applicationId, input.subjectType, input.subjectId),
            SK: sk(input.process),
          },
          UpdateExpression: updateExpression,
          ExpressionAttributeNames: names,
          ExpressionAttributeValues: values,
          ReturnValues: 'ALL_NEW',
        })
      );

      return toStateRecord(res.Attributes!);
    },

    async getState(applicationId, subjectType, subjectId, process) {
      const res = await ddb.send(
        new GetCommand({
          TableName: tableName,
          Key: {
            PK: pk(applicationId, subjectType, subjectId),
            SK: sk(process),
          },
        })
      );
      return res.Item ? toStateRecord(res.Item) : null;
    },

    async listStatesForSubject(applicationId, subjectType, subjectId) {
      const res = await ddb.send(
        new QueryCommand({
          TableName: tableName,
          KeyConditionExpression: 'PK = :pk AND begins_with(SK, :sk)',
          ExpressionAttributeValues: {
            ':pk': pk(applicationId, subjectType, subjectId),
            ':sk': 'PROC#',
          },
        })
      );
      return (res.Items ?? []).map(toStateRecord);
    },

    async getStatesByRunId(applicationId, runId) {
      const res = await ddb.send(
        new QueryCommand({
          TableName: tableName,
          IndexName: 'GSI1',
          KeyConditionExpression: 'GSI1PK = :pk',
          ExpressionAttributeValues: { ':pk': gsi1pk(runId) },
        })
      );
      // GSI1 is keyed by runId alone (not applicationId) — filter here so a
      // run id from another tenant never leaks a record back to the caller.
      return (res.Items ?? [])
        .map(toStateRecord)
        .filter((record) => record.applicationId === applicationId);
    },
  };
}
