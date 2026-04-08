import type { Context } from 'hono'
import type { Env, JwtPayload } from '../types'
import { signToken } from '../services/jwt'
import { checkRateLimit } from '../services/rate-limit'

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

type SessionEnv = { Bindings: Env; Variables: { jwtPayload: JwtPayload } }

export async function sessionRoute(c: Context<SessionEnv>) {
  const body = await c.req.json<{ userId?: string; sessionId?: string }>()

  if (!body.userId || !UUID_RE.test(body.userId)) {
    return c.json({ error: 'Invalid or missing userId (must be UUID v4)' }, 400)
  }
  if (!body.sessionId || !UUID_RE.test(body.sessionId)) {
    return c.json({ error: 'Invalid or missing sessionId (must be UUID v4)' }, 400)
  }

  const ip = c.req.header('CF-Connecting-IP') ?? c.req.header('x-forwarded-for') ?? 'unknown'
  const rateCheck = await checkRateLimit(c.env.RATE_LIMITER, `session:${ip}`, 10, 3_600_000)
  if (!rateCheck.allowed) {
    const retryAfter = Math.ceil((rateCheck.retryAfterMs ?? 60_000) / 1000)
    return c.json({ error: 'Rate limit exceeded' }, { status: 429, headers: { 'Retry-After': String(retryAfter) } })
  }

  const now = Math.floor(Date.now() / 1000)
  const payload: JwtPayload = {
    sub: body.userId,
    sessionId: body.sessionId,
    iat: now,
    exp: now + 86400,
  }

  const token = await signToken(payload, c.env.JWT_SECRET)

  return c.json({
    token,
    sessionId: body.sessionId,
    expiresAt: new Date((now + 86400) * 1000).toISOString(),
  })
}
