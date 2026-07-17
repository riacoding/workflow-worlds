import { readFileSync } from 'node:fs'

interface StackOutputs {
  TableName: string
  QueueUrl: string
  QueueArn: string
  QueueName: string
  SchedulerGroupName: string
  SchedulerRoleArn: string
  Region: string
}

const outputsPath = process.argv[2] ?? 'cdk-outputs.json'

let raw: string
try {
  raw = readFileSync(outputsPath, 'utf8')
} catch {
  console.error(
    `Could not read ${outputsPath}. Run \`pnpm deploy\` first (it writes this file via --outputs-file), ` +
      'or pass an explicit path: `pnpm print-env <path-to-outputs.json>`.',
  )
  process.exit(1)
}

const allOutputs = JSON.parse(raw) as Record<string, Partial<StackOutputs>>
const stackNames = Object.keys(allOutputs)

if (stackNames.length === 0) {
  console.error(`No stacks found in ${outputsPath}.`)
  process.exit(1)
}

if (stackNames.length > 1) {
  console.error(
    `Multiple stacks found in ${outputsPath} (${stackNames.join(', ')}). ` +
      'Deploy one stack at a time, or point print-env at a specific outputs file.',
  )
  process.exit(1)
}

const outputs = allOutputs[stackNames[0]!] as StackOutputs

const lines = [
  'export WORKFLOW_AWS_LOCAL=false',
  'export WORKFLOW_AWS_AUTO_PROVISION=false',
  `export WORKFLOW_AWS_REGION=${outputs.Region}`,
  `export WORKFLOW_DYNAMODB_TABLE_NAME=${outputs.TableName}`,
  `export WORKFLOW_SQS_QUEUE_URL=${outputs.QueueUrl}`,
  `export WORKFLOW_SQS_QUEUE_ARN=${outputs.QueueArn}`,
  `export WORKFLOW_SCHEDULER_GROUP_NAME=${outputs.SchedulerGroupName}`,
  `export WORKFLOW_SCHEDULER_ROLE_ARN=${outputs.SchedulerRoleArn}`,
]

console.log(lines.join('\n'))
