/**
 * Event Sourcing Contract Tests for AWS World
 */

import { eventSourcingTests } from '@workflow-worlds/testing';
import { createStorage } from './setup.js';

eventSourcingTests({ createStorage });
