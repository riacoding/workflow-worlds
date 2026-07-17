/**
 * Serialization Tests for AWS World
 */

import { serializationTests } from '@workflow-worlds/testing';
import { createStorage } from './setup.js';

serializationTests({ createStorage });
