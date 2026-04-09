import type { Context } from 'hono'
import type { Env, JwtPayload } from '../types'
import { getManifest } from '../services/manifest'

type ManifestEnv = { Bindings: Env; Variables: { jwtPayload: JwtPayload } }

export async function manifestRoute(c: Context<ManifestEnv>) {
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
