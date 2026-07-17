/**
 * Output Preservation Tests for AWS World
 */

import { outputPreservationTests } from '@workflow-worlds/testing';
import { createStorage } from './setup.js';

outputPreservationTests({ createStorage });
