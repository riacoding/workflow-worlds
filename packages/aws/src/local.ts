/**
 * Local development mode: auto-starts a LocalStack container so the AWS
 * World can run against emulated DynamoDB/SQS without touching real AWS.
 *
 * Enabled via WORKFLOW_AWS_LOCAL. Mirrors the container setup used by the
 * test suite (test/setup.ts), but lives in production src/ so it also works
 * for consumers of the published package.
 *
 * The container is bound to a fixed host port — derived from
 * WORKFLOW_AWS_ENDPOINT when the caller already set one, else LocalStack's
 * own default port — instead of testcontainers' usual random port
 * assignment, so a caller-provided endpoint can never point at a different
 * port than the container actually bound to. It also opts into
 * testcontainers' reuse feature (content-hash matched: image + ports + env),
 * so repeated process starts (e.g. a dev server restart) attach to the
 * already-running container instead of starting a duplicate — which means
 * this deliberately does NOT stop the container on process exit (SIGINT/
 * SIGTERM) the way earlier versions did; stopping it would defeat reuse,
 * since every restart would then find nothing to attach to and pay for a
 * full cold start again. The container outlives the process on purpose;
 * stop it yourself with `docker stop`/`docker rm` when you're done with it.
 * Note: if WORKFLOW_AWS_ENDPOINT's port changes between runs, the old
 * container is left running rather than reused or stopped — same manual
 * cleanup applies.
 */

import { LocalstackContainer } from '@testcontainers/localstack'
import { debug } from './utils.js'

// LocalStack's own container-internal port. @testcontainers/localstack exports
// this as LOCALSTACK_PORT internally, but doesn't re-export it from the
// package root, so it's duplicated here rather than imported.
const LOCALSTACK_PORT = 4566

let startPromise: Promise<{ endpoint: string }> | null = null

function extractPort(url: string | undefined): number | undefined {
  if (!url) return undefined
  try {
    const port = new URL(url).port
    return port ? Number(port) : undefined
  } catch {
    return undefined
  }
}

export interface StartLocalStackOptions {
  image?: string
  /** The caller's already-resolved WORKFLOW_AWS_ENDPOINT, if any — used only to derive the fixed port. */
  endpoint?: string
}

export function startLocalStack(options: StartLocalStackOptions = {}): Promise<{ endpoint: string }> {
  if (startPromise) return startPromise

  const image = options.image ?? 'localstack/localstack:3'
  const port = extractPort(options.endpoint) ?? LOCALSTACK_PORT

  startPromise = (async () => {
    debug('WORKFLOW_AWS_LOCAL is set — starting LocalStack container on port', port)
    const container = await new LocalstackContainer(image)
      .withExposedPorts({ container: LOCALSTACK_PORT, host: port })
      .withReuse()
      .start()
    const endpoint = container.getConnectionUri()

    process.stderr.write(`[aws-world] LocalStack started at ${endpoint} (DynamoDB + SQS emulated)\n`)

    return { endpoint }
  })()

  return startPromise
}
