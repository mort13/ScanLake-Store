import type { Context } from 'hono'
import type { Env, JwtPayload } from '../types'
import { checkRateLimit } from '../services/rate-limit'
import { buildR2Key, writeParquetToR2 } from '../services/r2'

type UploadEnv = { Bindings: Env; Variables: { jwtPayload: JwtPayload } }

export async function uploadRoute(c: Context<UploadEnv>) {
  const jwt = c.get('jwtPayload')
  const userId = jwt.sub

  const rateCheck = await checkRateLimit(c.env.RATE_LIMITER, `upload:${userId}`, 100, 3_600_000)
  if (!rateCheck.allowed) {
    const retryAfter = Math.ceil((rateCheck.retryAfterMs ?? 60_000) / 1000)
    return c.json({ error: 'Rate limit exceeded' }, { status: 429, headers: { 'Retry-After': String(retryAfter) } })
  }

  const formData = await c.req.formData()
  const sessionId = formData.get('sessionId') as string | null
  const batchNumberRaw = formData.get('batchNumber') as string | null
  const scansFile = formData.get('scans') as File | null
  const compositionsFile = formData.get('compositions') as File | null

  if (!sessionId) return c.json({ error: 'Missing sessionId' }, 400)
  const batchNumber = parseInt(batchNumberRaw ?? '', 10)
  if (isNaN(batchNumber) || batchNumber < 1) return c.json({ error: 'Invalid batchNumber' }, 400)
  if (!scansFile) return c.json({ error: 'Missing scans file' }, 400)
  if (!compositionsFile) return c.json({ error: 'Missing compositions file' }, 400)

  const formUserId = formData.get('userId') as string | null
  if (formUserId && formUserId !== userId) {
    console.warn(`userId mismatch: form="${formUserId}" jwt="${userId}"`)
  }

  const scansData = await scansFile.arrayBuffer()
  const compositionsData = await compositionsFile.arrayBuffer()

  const scansKey = buildR2Key('scans', userId, sessionId, batchNumber)
  const compositionsKey = buildR2Key('compositions', userId, sessionId, batchNumber)

  try {
    await Promise.all([
      writeParquetToR2(c.env.SCANLAKE_BUCKET, scansKey, scansData),
      writeParquetToR2(c.env.SCANLAKE_BUCKET, compositionsKey, compositionsData),
    ])
  } catch (err) {
    console.error('R2 write failed:', err)
    return c.json({ error: 'Storage write failed' }, 500)
  }

  return c.json({ ok: true, keys: [scansKey, compositionsKey] })
}
