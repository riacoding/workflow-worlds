/**
 * Hook Cleanup Tests for AWS World
 */

import { hookCleanupTests } from '@workflow-worlds/testing';
import { createStorage } from './setup.js';

hookCleanupTests({ createStorage });
