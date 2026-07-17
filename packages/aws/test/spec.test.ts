/**
 * Standard Test Suite for AWS World
 *
 * Runs the @workflow/world-testing integration tests (addition, idempotency,
 * hooks, errors, nullByte) against the DynamoDB/SQS/AppSync-backed world.
 */

import { createTestSuite } from '@workflow/world-testing';
import { worldPath } from './setup.js';

createTestSuite(worldPath);
