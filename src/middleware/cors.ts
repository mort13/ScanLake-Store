import { cors } from 'hono/cors'
import type { Env } from '../types'
import type { MiddlewareHandler } from 'hono'

/** CORS for internal (first-party) routes — origin locked to ALLOWED_ORIGIN env var. */
export const corsMiddleware: MiddlewareHandler<{ Bindings: Env }> = (c, next) => {
  const handler = cors({
    origin: c.env.ALLOWED_ORIGIN,
    allowMethods: ['POST', 'OPTIONS'],
    allowHeaders: ['Authorization', 'Content-Type'],
    maxAge: 86400,
  })
  return handler(c, next)
}

/**
 * CORS for external (third-party) routes — allows any origin so that
 * browser-based third-party clients can call the API.  Per-key origin
 * restrictions are enforced separately in apiKeyMiddleware.
 */
export const externalCorsMiddleware: MiddlewareHandler<{ Bindings: Env }> = (_c, next) => {
  const handler = cors({
    origin: '*',
    allowMethods: ['POST', 'GET', 'OPTIONS'],
    allowHeaders: ['X-API-Key', 'Content-Type'],
    maxAge: 86400,
  })
  return handler(_c, next)
}
