import { cors } from 'hono/cors'
import type { Env } from '../types'
import type { MiddlewareHandler } from 'hono'

export const corsMiddleware: MiddlewareHandler<{ Bindings: Env }> = (c, next) => {
  const handler = cors({
    origin: c.env.ALLOWED_ORIGIN,
    allowMethods: ['POST', 'OPTIONS'],
    allowHeaders: ['Authorization', 'Content-Type'],
    maxAge: 86400,
  })
  return handler(c, next)
}
