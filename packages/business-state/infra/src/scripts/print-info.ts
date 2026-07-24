import { readFileSync } from 'node:fs'

const outputsPath = process.argv[2] ?? 'cdk-outputs.json'

let raw: string
try {
  raw = readFileSync(outputsPath, 'utf8')
} catch {
  console.error(
    `Could not read ${outputsPath}. Run \`pnpm deploy\` first (it writes this file via --outputs-file), ` +
      'or pass an explicit path: `pnpm print-info <path-to-outputs.json>`.',
  )
  process.exit(1)
}

const allOutputs = JSON.parse(raw) as Record<string, Record<string, string>>
const stackNames = Object.keys(allOutputs)

if (stackNames.length === 0) {
  console.error(`No stacks found in ${outputsPath}.`)
  process.exit(1)
}
if (stackNames.length > 1) {
  console.error(
    `Multiple stacks found in ${outputsPath} (${stackNames.join(', ')}). ` +
      'Deploy one stack at a time, or point print-info at a specific outputs file.',
  )
  process.exit(1)
}

const outputs = allOutputs[stackNames[0]!]!

console.log(`API URL: ${outputs.ApiUrl}`)
console.log(`Table:   ${outputs.TableName}`)
console.log('')
console.log('API key ids — key values are secrets and are not in this file. Retrieve each with:')
for (const [key, value] of Object.entries(outputs)) {
  if (!key.startsWith('ApiKeyId')) continue
  const appName = key.replace(/^ApiKeyId-?/, '')
  console.log(`  ${appName}: ${value}`)
  console.log(`    aws apigateway get-api-key --api-key ${value} --include-value --query value --output text`)
}
