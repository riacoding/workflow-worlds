/**
 * Local development mode: auto-starts a LocalStack container so the AWS
 * World can run against emulated DynamoDB/SQS without touching real AWS.
 *
 * Enabled via WORKFLOW_AWS_LOCAL. Mirrors the container setup used by the
 * test suite (test/setup.ts), but lives in production src/ so it also works
 * for consumers of the published package.
 */

import { LocalstackContainer } from '@testcontainers/localstack'
import { debug } from './utils.js'

let startPromise: Promise<{ endpoint: string }> | null = null

export function startLocalStack(image = 'localstack/localstack:3'): Promise<{ endpoint: string }> {
  if (startPromise) return startPromise

  startPromise = (async () => {
    debug('WORKFLOW_AWS_LOCAL is set — starting LocalStack container...')
    const container = await new LocalstackContainer(image).start()
    const endpoint = container.getConnectionUri()

    process.stderr.write(`[aws-world] LocalStack started at ${endpoint} (DynamoDB + SQS emulated)\n`)

    const shutdown = async () => {
      await container.stop().catch(() => {})
    }
    process.once('SIGINT', shutdown)
    process.once('SIGTERM', shutdown)

    return { endpoint }
  })()

  return startPromise
}
