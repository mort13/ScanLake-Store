import type { MiddlewareHandler } from 'hono'
import type { Env, JwtPayload } from '../types'
import { verifyToken } from '../services/jwt'

type AuthEnv = { Bindings: Env; Variables: { jwtPayload: JwtPayload } }

export const authMiddleware: MiddlewareHandler<AuthEnv> = async (c, next) => {
  const header = c.req.header('Authorization')
  if (!header?.startsWith('Bearer ')) {
    return c.json({ error: 'Missing or malformed Authorization header' }, 401)
  }

  const token = header.slice(7)
  try {
    const payload = await verifyToken(token, c.env.JWT_SECRET)
    c.set('jwtPayload', payload)
  } catch (err) {
    const message = err instanceof Error ? err.message : 'Invalid token'
    return c.json({ error: message }, 401)
  }

  await next()
}
