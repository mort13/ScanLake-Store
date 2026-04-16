import { Hono } from 'hono'
import type { Env, JwtPayload, ApiKeyMeta } from './types'
import { corsMiddleware, externalCorsMiddleware } from './middleware/cors'
import { authMiddleware } from './middleware/auth'
import { apiKeyMiddleware } from './middleware/apiKey'
import { formatEnforcerMiddleware } from './middleware/formatEnforcer'
import { sessionRoute } from './routes/session'
import { uploadRoute } from './routes/upload'
import { manifestRoute } from './routes/manifest'
import { externalUploadRoute } from './routes/external/upload'
import { externalManifestRoute } from './routes/external/manifest'

export { RateLimiter } from './durable-objects/RateLimiter'

type AppEnv = {
  Bindings: Env
  Variables: { jwtPayload: JwtPayload; apiKeyMeta: ApiKeyMeta; apiKeyHash: string }
}

const app = new Hono<AppEnv>()

// ── External (third-party) routes ─────────────────────────────────────────────
// These must be registered before the wildcard internal CORS middleware so that
// OPTIONS preflight requests are handled by externalCorsMiddleware and not by
// the internal one (which would return the wrong Access-Control-Allow-Origin).
app.use('/api/external/*', externalCorsMiddleware)
app.use('/api/external/*', apiKeyMiddleware)
app.post('/api/external/upload', formatEnforcerMiddleware, externalUploadRoute)
app.get('/api/external/manifest', externalManifestRoute)

// ── Internal (first-party) routes ─────────────────────────────────────────────
app.use('*', (c, next) => {
  // Skip internal CORS for external paths — those already have their own handler.
  if (c.req.path.startsWith('/api/external')) return next()
  return corsMiddleware(c, next)
})

app.post('/api/session', sessionRoute)
app.post('/api/upload', authMiddleware, uploadRoute)
app.get('/api/manifest', manifestRoute)

app.all('*', (c) => c.json({ error: 'Not found' }, 404))

export default app
