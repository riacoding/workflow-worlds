#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# recreate-table.sh — inspect, delete, and recreate the AWS World's DynamoDB
# table (dev convenience for wiping all workflow data without hand-deleting
# every item).
#
# Usage:
#   ./scripts/recreate-table.sh [table-name] [options]
#
# Examples:
#   ./scripts/recreate-table.sh                        # reads table name from cdk-outputs.json
#   ./scripts/recreate-table.sh myapp-dev-workflow
#   ./scripts/recreate-table.sh myapp-dev-workflow --yes
#   ./scripts/recreate-table.sh myapp-dev-workflow --inspect-only
#
# Options:
#   --yes, -y        Skip the interactive "type the table name to confirm" prompt.
#   --inspect-only   Only print the table's current details; do not delete/recreate.
#   --region <r>     AWS region (default: $AWS_REGION / $WORKFLOW_AWS_REGION / cdk-outputs.json).
#   -h, --help       Show this help message.
#
# IMPORTANT — CloudFormation drift: this table is normally managed by the CDK
# stack in ../src/lib/workflow-aws-stack.ts. This script recreates the table
# OUTSIDE of CloudFormation, so it deliberately mirrors that stack's schema
# exactly (see the "Table schema" block below — keep the two in sync). As long
# as the name and schema match what the stack expects, a subsequent
# `cdk diff`/`deploy` should show no changes for the table. If you ever change
# the table's shape in workflow-aws-stack.ts, update this script to match.
# =============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INFRA_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUTS_FILE="$INFRA_DIR/cdk-outputs.json"

# --- Colors ------------------------------------------------------------------

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

log()         { echo -e "${CYAN}[recreate-table]${NC} $*"; }
log_step()    { echo -e "\n${CYAN}${BOLD}==> $*${NC}"; }
log_success() { echo -e "${GREEN}[recreate-table]${NC} $*"; }
log_warn()    { echo -e "${YELLOW}[recreate-table]${NC} $*"; }
log_error()   { echo -e "${RED}[recreate-table]${NC} $*" >&2; }

usage() {
  sed -n '3,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

# --- Parse arguments -----------------------------------------------------

TABLE_NAME=""
SKIP_CONFIRM=false
INSPECT_ONLY=false
REGION="${AWS_REGION:-${WORKFLOW_AWS_REGION:-}}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --yes|-y)
      SKIP_CONFIRM=true
      shift
      ;;
    --inspect-only)
      INSPECT_ONLY=true
      shift
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    -*)
      log_error "Unknown option: $1"
      usage
      exit 1
      ;;
    *)
      TABLE_NAME="$1"
      shift
      ;;
  esac
done

if ! command -v aws &>/dev/null; then
  log_error "AWS CLI not found. Install it: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
  exit 1
fi

# --- Resolve table name + region from cdk-outputs.json if not given ---------

if [[ -z "$TABLE_NAME" || -z "$REGION" ]]; then
  if [[ -f "$OUTPUTS_FILE" ]]; then
    RESOLVED=$(node -e "
      const fs = require('fs');
      const outputs = JSON.parse(fs.readFileSync('$OUTPUTS_FILE', 'utf8'));
      const stacks = Object.keys(outputs);
      if (stacks.length !== 1) process.exit(1);
      const o = outputs[stacks[0]];
      process.stdout.write(JSON.stringify({ tableName: o.TableName, region: o.Region }));
    " 2>/dev/null) || RESOLVED=""

    if [[ -n "$RESOLVED" ]]; then
      DEFAULT_TABLE=$(node -e "process.stdout.write(JSON.parse(process.argv[1]).tableName || '')" "$RESOLVED")
      DEFAULT_REGION=$(node -e "process.stdout.write(JSON.parse(process.argv[1]).region || '')" "$RESOLVED")
      TABLE_NAME="${TABLE_NAME:-$DEFAULT_TABLE}"
      REGION="${REGION:-$DEFAULT_REGION}"
    fi
  fi
fi

if [[ -z "$TABLE_NAME" ]]; then
  log_error "No table name given and could not resolve one from $OUTPUTS_FILE."
  usage
  exit 1
fi

REGION="${REGION:-us-east-1}"

AWS_ARGS=(--region "$REGION")

log "Table:  $TABLE_NAME"
log "Region: $REGION"

# =============================================================================
# Step 1: Inspect
# =============================================================================
log_step "Step 1: Inspecting current table"

if ! DESCRIBE_JSON=$(aws dynamodb describe-table --table-name "$TABLE_NAME" "${AWS_ARGS[@]}" 2>&1); then
  log_error "Could not describe table '$TABLE_NAME' in region $REGION:"
  echo "$DESCRIBE_JSON" >&2
  exit 1
fi

TTL_JSON=$(aws dynamodb describe-time-to-live --table-name "$TABLE_NAME" "${AWS_ARGS[@]}" 2>/dev/null || echo '{}')

node -e "
  const table = JSON.parse(process.argv[1]).Table;
  const ttl = JSON.parse(process.argv[2]).TimeToLiveDescription;
  console.log('  Status:        ', table.TableStatus);
  console.log('  Item count:    ', table.ItemCount, '(approximate — AWS updates this ~every 6h)');
  console.log('  Size (bytes):  ', table.TableSizeBytes);
  console.log('  Billing mode:  ', table.BillingModeSummary?.BillingMode ?? 'PROVISIONED');
  console.log('  Key schema:    ', JSON.stringify(table.KeySchema));
  console.log('  GSIs:          ', (table.GlobalSecondaryIndexes || []).map((g) => g.IndexName).join(', ') || '(none)');
  console.log('  TTL attribute: ', ttl?.AttributeName ?? '(disabled)', ttl?.TimeToLiveStatus ? \`[\${ttl.TimeToLiveStatus}]\` : '');
  console.log('  ARN:           ', table.TableArn);
" "$DESCRIBE_JSON" "$TTL_JSON"

if [[ "$INSPECT_ONLY" == "true" ]]; then
  log_success "Inspect-only mode — not deleting. Done."
  exit 0
fi

# =============================================================================
# Step 2: Confirm
# =============================================================================
log_step "Step 2: Confirm deletion"

log_warn "This PERMANENTLY DELETES every item in '$TABLE_NAME' (region $REGION)."
log_warn "This table is normally managed by CDK (packages/aws/infra) — deleting/recreating it"
log_warn "outside CloudFormation is safe only as long as the recreated schema below matches the"
log_warn "stack's definition exactly."
if [[ "$TABLE_NAME" == *prod* ]]; then
  log_error "Table name contains 'prod' — refusing to proceed automatically."
  log_error "If you really mean to do this, delete/recreate it manually."
  exit 1
fi

if [[ "$SKIP_CONFIRM" != "true" ]]; then
  read -r -p "Type the table name to confirm deletion: " CONFIRM_NAME
  if [[ "$CONFIRM_NAME" != "$TABLE_NAME" ]]; then
    log_error "Confirmation did not match '$TABLE_NAME'. Aborting."
    exit 1
  fi
fi

# =============================================================================
# Step 3: Delete
# =============================================================================
log_step "Step 3: Deleting table"

aws dynamodb delete-table --table-name "$TABLE_NAME" "${AWS_ARGS[@]}" >/dev/null
log "Waiting for deletion to complete..."
aws dynamodb wait table-not-exists --table-name "$TABLE_NAME" "${AWS_ARGS[@]}"
log_success "Table deleted."

# =============================================================================
# Step 4: Recreate
# =============================================================================
log_step "Step 4: Recreating table"

# --- Table schema ------------------------------------------------------------
# Mirrors packages/aws/infra/src/lib/workflow-aws-stack.ts's Table construct
# (and packages/aws/src/aws.ts's ensureTable(), which the stack itself mirrors):
# PK/SK partition+sort key, GSI1 + GSI2 (ALL projection), PAY_PER_REQUEST,
# TTL enabled on the `ttl` attribute after creation.
# ------------------------------------------------------------------------------

aws dynamodb create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions \
    AttributeName=PK,AttributeType=S \
    AttributeName=SK,AttributeType=S \
    AttributeName=GSI1PK,AttributeType=S \
    AttributeName=GSI1SK,AttributeType=S \
    AttributeName=GSI2PK,AttributeType=S \
    AttributeName=GSI2SK,AttributeType=S \
  --key-schema \
    AttributeName=PK,KeyType=HASH \
    AttributeName=SK,KeyType=RANGE \
  --global-secondary-indexes \
    'IndexName=GSI1,KeySchema=[{AttributeName=GSI1PK,KeyType=HASH},{AttributeName=GSI1SK,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
    'IndexName=GSI2,KeySchema=[{AttributeName=GSI2PK,KeyType=HASH},{AttributeName=GSI2SK,KeyType=RANGE}],Projection={ProjectionType=ALL}' \
  --billing-mode PAY_PER_REQUEST \
  "${AWS_ARGS[@]}" >/dev/null

log "Waiting for table to become active..."
aws dynamodb wait table-exists --table-name "$TABLE_NAME" "${AWS_ARGS[@]}"

log "Enabling TTL on the 'ttl' attribute..."
aws dynamodb update-time-to-live \
  --table-name "$TABLE_NAME" \
  --time-to-live-specification "Enabled=true,AttributeName=ttl" \
  "${AWS_ARGS[@]}" >/dev/null

log_success "Table recreated."

# =============================================================================
# Step 5: Confirm final state
# =============================================================================
log_step "Step 5: Final state"

aws dynamodb describe-table --table-name "$TABLE_NAME" "${AWS_ARGS[@]}" \
  --query 'Table.{Status:TableStatus,Billing:BillingModeSummary.BillingMode,GSIs:GlobalSecondaryIndexes[].IndexName}' \
  --output table

log_success "Done. '$TABLE_NAME' is empty and ready."
