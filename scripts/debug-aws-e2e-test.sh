#!/usr/bin/env bash
set -euo pipefail

# Debug helper: runs ONE upstream e2e test (by name pattern) against the aws
# world with full [aws-world] debug logging, and prints the dev server's log
# inline at the end so nothing is buried in a background process.
#
# What it does, every run:
#   1. Rebuilds packages/aws and pushes the fresh dist/ into the already-
#      installed upstream copy (fast — skips the full pack+install cycle).
#   2. Starts (or reuses) the e2e-localstack Docker container.
#   3. Purges the SQS queue and drops+recreates the DynamoDB table, so old
#      messages/state from a previous run can't contaminate this one.
#   4. Starts the upstream Next.js dev server with WORKFLOW_DEBUG=aws-world.
#   5. Runs `vitest -t <pattern>` against packages/core/e2e/e2e.test.ts —
#      with the SAME world env vars exported in this SAME script, so the
#      test process's own `start()`/`queue()` calls use the real aws world
#      too (a separate `pnpm vitest run` invocation in a fresh shell would
#      silently fall back to world-local and give misleading results).
#   6. Prints the dev server's [aws-world] debug lines + errors after the
#      test finishes.
#
# Requires: `pnpm e2e:aws` has been run at least once already (so
# .e2e-upstream exists with dependencies installed), and Docker is running.
#
# Usage:
#   ./scripts/debug-aws-e2e-test.sh                    # runs "sleepingWorkflow$"
#   ./scripts/debug-aws-e2e-test.sh "hookWorkflow$"     # runs a different test
#
# The dev server is left running after the test so you can poke at it
# further (curl it, inspect DynamoDB/SQS, etc.) — stop it with:
#   pkill -f "next dev --turbopack"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UPSTREAM_DIR="$ROOT_DIR/.e2e-upstream"
APP_DIR="$UPSTREAM_DIR/workbench/nextjs-turbopack"
TEST_PATTERN="${1:-sleepingWorkflow$}"
DEV_LOG="$ROOT_DIR/.debug-dev-server.log"

if [[ ! -d "$UPSTREAM_DIR" ]]; then
  echo "ERROR: $UPSTREAM_DIR not found. Run 'pnpm e2e:aws' once first to set it up." >&2
  exit 1
fi

echo "==> [1/6] Building @workflow-worlds/aws..."
(cd "$ROOT_DIR/packages/aws" && pnpm build)

TARBALL_NODE_MODULES=$(find "$UPSTREAM_DIR/node_modules/.pnpm" -maxdepth 1 -iname "@workflow-worlds+aws@file*" | head -1)
if [[ -z "$TARBALL_NODE_MODULES" ]]; then
  echo "ERROR: could not find an installed @workflow-worlds/aws under .e2e-upstream. Run 'pnpm e2e:aws' once first." >&2
  exit 1
fi
TARBALL_DIST="$TARBALL_NODE_MODULES/node_modules/@workflow-worlds/aws/dist"
cp -r "$ROOT_DIR/packages/aws/dist/"* "$TARBALL_DIST/"
echo "    pushed fresh build into $TARBALL_DIST"

echo "==> [2/6] Ensuring LocalStack is up..."
if ! docker ps --filter "name=e2e-localstack" --format '{{.Names}}' | grep -q e2e-localstack; then
  docker rm e2e-localstack >/dev/null 2>&1 || true
  docker run -d --name e2e-localstack -p 4566:4566 localstack/localstack:3 >/dev/null
  for i in $(seq 1 30); do
    curl -s http://localhost:4566/_localstack/health 2>/dev/null | grep -q '"dynamodb"' && break
    sleep 2
  done
fi

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
QUEUE_URL="http://sqs.us-west-2.localhost.localstack.cloud:4566/000000000000/workflow-queue"

echo "==> [3/6] Purging queue + resetting table for a clean run..."
aws --endpoint-url=http://localhost:4566 --region us-west-2 sqs purge-queue --queue-url "$QUEUE_URL" >/dev/null 2>&1 || true
aws --endpoint-url=http://localhost:4566 --region us-west-2 dynamodb delete-table --table-name workflow >/dev/null 2>&1 || true
for i in $(seq 1 20); do
  aws --endpoint-url=http://localhost:4566 --region us-west-2 dynamodb describe-table --table-name workflow >/dev/null 2>&1 || break
  sleep 1
done

echo "==> [4/6] Starting dev server (log: $DEV_LOG)..."
pkill -f "next dev --turbopack" 2>/dev/null || true
sleep 1
rm -f "$DEV_LOG"

export WORKFLOW_TARGET_WORLD=@workflow-worlds/aws
export WORKFLOW_AWS_ENDPOINT=http://localhost:4566
export WORKFLOW_AWS_REGION=us-west-2
export WORKFLOW_AWS_ACCESS_KEY_ID=test
export WORKFLOW_AWS_SECRET_ACCESS_KEY=test
export WORKFLOW_DEBUG=aws-world
export WORKFLOW_PUBLIC_MANIFEST=1
export WORKFLOW_SERVICE_URL=http://localhost:3000
export NODE_OPTIONS=--enable-source-maps
export DEPLOYMENT_URL=http://localhost:3000
export APP_NAME=nextjs-turbopack

(cd "$APP_DIR" && nohup pnpm dev > "$DEV_LOG" 2>&1 &)

for i in $(seq 1 30); do
  if curl -s -o /dev/null -w '%{http_code}' http://localhost:3000 | grep -qE '^[23]'; then
    echo "    ready after ${i}s"
    break
  fi
  sleep 1
done

echo "==> [5/6] Running test matching: $TEST_PATTERN"
echo ""
cd "$UPSTREAM_DIR"
set +e
pnpm vitest run packages/core/e2e/e2e.test.ts -t "$TEST_PATTERN" --config vitest.config.ts --reporter=verbose
TEST_EXIT=$?
set -e

echo ""
echo "==> [6/6] Dev server debug log ($DEV_LOG) — [aws-world] lines + errors:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
grep -E "\[aws-world\]|⨯|Error|POST /" "$DEV_LOG" || echo "(no matching lines)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Full dev server log: $DEV_LOG"
echo "Dev server left running — stop it with: pkill -f 'next dev --turbopack'"

exit $TEST_EXIT
