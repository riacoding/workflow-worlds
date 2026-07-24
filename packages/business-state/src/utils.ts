/**
 * Business State Utility Functions
 */

// =============================================================================
// Debug Logging
// =============================================================================

/**
 * Creates a debug logger that writes to stderr when enabled.
 *
 * Enable with WORKFLOW_DEBUG environment variable:
 * - WORKFLOW_DEBUG=1 or WORKFLOW_DEBUG=true - enables all debug output
 * - WORKFLOW_DEBUG=business-state - enables only this namespace
 * - WORKFLOW_DEBUG=redis,business-state - enables multiple namespaces
 *
 * Writes to stderr to avoid interfering with CLI stdout JSON parsing.
 */
function createDebugLogger(namespace: string) {
  return (...args: unknown[]) => {
    const debug = process.env.WORKFLOW_DEBUG;
    if (!debug) return;

    const enabled =
      debug === '1' ||
      debug === 'true' ||
      debug === '*' ||
      debug.split(',').some((ns) => ns.trim() === namespace);

    if (!enabled) return;

    const timestamp = new Date().toISOString();
    const prefix = `[${timestamp}] [${namespace}]`;

    const message = args
      .map((arg) =>
        typeof arg === 'object' ? JSON.stringify(arg, null, 2) : String(arg)
      )
      .join(' ');

    process.stderr.write(`${prefix} ${message}\n`);
  };
}

/**
 * Debug logger for business-state.
 * Enable with WORKFLOW_DEBUG=business-state or WORKFLOW_DEBUG=1
 */
export const debug = createDebugLogger('business-state');
