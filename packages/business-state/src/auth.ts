/**
 * Tenancy resolution: maps the API Gateway API key id (never client-supplied
 * data) to the applicationId that key belongs to. The map is baked into the
 * Lambda's environment at CDK synth time from the `-c apps=...` context
 * param — see infra/src/lib/business-state-stack.ts.
 */

let cachedMap: Record<string, string> | undefined;

function loadMap(): Record<string, string> {
  if (cachedMap) return cachedMap;
  const raw = process.env.BUSINESS_STATE_APP_KEY_MAP ?? '{}';
  try {
    cachedMap = JSON.parse(raw) as Record<string, string>;
  } catch {
    cachedMap = {};
  }
  return cachedMap;
}

export function resolveApplicationId(apiKeyId: string | undefined): string | null {
  if (!apiKeyId) return null;
  return loadMap()[apiKeyId] ?? null;
}
