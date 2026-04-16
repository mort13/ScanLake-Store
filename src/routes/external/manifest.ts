import type { Context } from 'hono'
import type { Env, ApiKeyMeta } from '../../types'
import { checkRateLimit } from '../../services/rate-limit'
import { getManifest } from '../../services/manifest'

type ExternalManifestEnv = {
  Bindings: Env
  Variables: { apiKeyMeta: ApiKeyMeta; apiKeyHash: string }
}

/** GET /api/external/manifest — rate-limited, API-key-gated manifest read. */
export async function externalManifestRoute(c: Context<ExternalManifestEnv>) {
  const meta = c.get('apiKeyMeta')
  const keyHash = c.get('apiKeyHash')

  const rateCheck = await checkRateLimit(
    c.env.RATE_LIMITER,
    `ext-key:${keyHash}`,
    meta.rateLimit,
    meta.rateLimitWindowMs,
  )
  if (!rateCheck.allowed) {
    const retryAfter = Math.ceil((rateCheck.retryAfterMs ?? 60_000) / 1000)
    return c.json(
      { error: 'API key rate limit exceeded' },
      { status: 429, headers: { 'Retry-After': String(retryAfter) } },
    )
  }

  try {
    const manifest = await getManifest(c.env.SCANLAKE_BUCKET)
    if (!manifest) {
      return c.json({ error: 'Manifest not found', files: [] }, 404)
    }
    return c.json(manifest)
  } catch (err) {
    console.error('Failed to retrieve manifest:', err)
    return c.json({ error: 'Failed to retrieve manifest' }, 500)
  }
}
