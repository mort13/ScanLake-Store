import type { Context } from 'hono'
import type { Env, ApiKeyMeta } from '../../types'
import { checkRateLimit } from '../../services/rate-limit'
import { buildR2Key, writeParquetToR2 } from '../../services/r2'
import { updateManifest } from '../../services/manifest'

/** Default IP-level request cap for external routes (per hour). */
const IP_RATE_LIMIT = 200
const IP_RATE_WINDOW_MS = 3_600_000

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

/** First four bytes of every valid Parquet file: "PAR1" */
const PARQUET_MAGIC = new Uint8Array([0x50, 0x41, 0x52, 0x31])

type ExternalUploadEnv = {
  Bindings: Env
  Variables: { apiKeyMeta: ApiKeyMeta; apiKeyHash: string }
}

function hasParquetMagic(buf: ArrayBuffer): boolean {
  if (buf.byteLength < 4) return false
  const view = new Uint8Array(buf, 0, 4)
  return view.every((b, i) => b === PARQUET_MAGIC[i])
}

/**
 * POST /api/external/upload
 *
 * Accepts the same multipart form fields as the internal upload route
 * but authenticates via an API key instead of a JWT.  The userId is
 * taken from the API key's ownerId so the caller cannot write data
 * under an arbitrary user.
 *
 * Rate limiting runs on two axes:
 *   - Per API key  (configurable via ApiKeyMeta.rateLimit / rateLimitWindowMs)
 *   - Per IP       (200 req / hour hard default — protects against key sharing)
 *
 * Format enforcement:
 *   - Required fields: sessionId (UUID v4), batchNumber (int ≥ 1), scans, compositions
 *   - Files must have the .parquet extension and valid PAR1 magic bytes
 *   - Each file must be ≤ apiKeyMeta.maxUploadBytes
 */
export async function externalUploadRoute(c: Context<ExternalUploadEnv>) {
  const meta = c.get('apiKeyMeta')
  const keyHash = c.get('apiKeyHash')
  const ip = c.req.header('CF-Connecting-IP') ?? c.req.header('x-forwarded-for') ?? 'unknown'

  // --- Rate limiting: API key ---
  const keyRateCheck = await checkRateLimit(
    c.env.RATE_LIMITER,
    `ext-key:${keyHash}`,
    meta.rateLimit,
    meta.rateLimitWindowMs,
  )
  if (!keyRateCheck.allowed) {
    const retryAfter = Math.ceil((keyRateCheck.retryAfterMs ?? 60_000) / 1000)
    return c.json(
      { error: 'API key rate limit exceeded' },
      { status: 429, headers: { 'Retry-After': String(retryAfter) } },
    )
  }

  // --- Rate limiting: IP ---
  const ipRateCheck = await checkRateLimit(
    c.env.RATE_LIMITER,
    `ext-ip:${ip}`,
    IP_RATE_LIMIT,
    IP_RATE_WINDOW_MS,
  )
  if (!ipRateCheck.allowed) {
    const retryAfter = Math.ceil((ipRateCheck.retryAfterMs ?? 60_000) / 1000)
    return c.json(
      { error: 'IP rate limit exceeded' },
      { status: 429, headers: { 'Retry-After': String(retryAfter) } },
    )
  }

  // --- Parse body ---
  const formData = await c.req.formData()
  const sessionId = formData.get('sessionId') as string | null
  const batchNumberRaw = formData.get('batchNumber') as string | null
  const scansFile = formData.get('scans') as File | null
  const compositionsFile = formData.get('compositions') as File | null
  const confidencesFile = formData.get('confidences') as File | null

  // --- Structural validation ---
  if (!sessionId || !UUID_RE.test(sessionId)) {
    return c.json({ error: 'Missing or invalid sessionId (must be UUID v4)' }, 400)
  }

  const batchNumber = parseInt(batchNumberRaw ?? '', 10)
  if (isNaN(batchNumber) || batchNumber < 1) {
    return c.json({ error: 'Invalid batchNumber (must be a positive integer)' }, 400)
  }

  if (!(scansFile instanceof File)) {
    return c.json({ error: 'Missing scans file' }, 400)
  }
  if (!(compositionsFile instanceof File)) {
    return c.json({ error: 'Missing compositions file' }, 400)
  }

  // --- File extension check ---
  const requiredFiles: [string, File][] = [
    ['scans', scansFile],
    ['compositions', compositionsFile],
    ...(confidencesFile instanceof File ? [['confidences', confidencesFile] as [string, File]] : []),
  ]

  for (const [name, file] of requiredFiles) {
    if (file.name && !file.name.endsWith('.parquet')) {
      return c.json({ error: `${name}: filename must end with .parquet` }, 422)
    }
  }

  // --- Per-file size enforcement ---
  const maxBytes = meta.maxUploadBytes
  for (const [name, file] of requiredFiles) {
    if (file.size > maxBytes) {
      return c.json(
        { error: `${name} file exceeds the per-file limit of ${maxBytes} bytes for this API key` },
        413,
      )
    }
  }

  // --- Read buffers and validate Parquet magic bytes ---
  const scansData = await scansFile.arrayBuffer()
  if (!hasParquetMagic(scansData)) {
    return c.json({ error: 'scans: not a valid Parquet file (missing PAR1 magic bytes)' }, 422)
  }

  const compositionsData = await compositionsFile.arrayBuffer()
  if (!hasParquetMagic(compositionsData)) {
    return c.json({ error: 'compositions: not a valid Parquet file (missing PAR1 magic bytes)' }, 422)
  }

  // The ownerId from the key determines the R2 path — callers cannot
  // choose an arbitrary userId.
  const userId = meta.ownerId
  const scansKey = buildR2Key('scans', userId, sessionId, batchNumber)
  const compositionsKey = buildR2Key('compositions', userId, sessionId, batchNumber)

  const writes: Promise<void>[] = [
    writeParquetToR2(c.env.SCANLAKE_BUCKET, scansKey, scansData),
    writeParquetToR2(c.env.SCANLAKE_BUCKET, compositionsKey, compositionsData),
  ]
  const manifestEntries: Array<{ key: string; type: 'scans' | 'compositions' | 'confidences' }> = [
    { key: scansKey, type: 'scans' },
    { key: compositionsKey, type: 'compositions' },
  ]
  const responseKeys = [scansKey, compositionsKey]

  if (confidencesFile instanceof File) {
    const confidencesData = await confidencesFile.arrayBuffer()
    if (!hasParquetMagic(confidencesData)) {
      return c.json(
        { error: 'confidences: not a valid Parquet file (missing PAR1 magic bytes)' },
        422,
      )
    }
    const confidencesKey = buildR2Key('confidences', userId, sessionId, batchNumber)
    writes.push(writeParquetToR2(c.env.SCANLAKE_BUCKET, confidencesKey, confidencesData))
    manifestEntries.push({ key: confidencesKey, type: 'confidences' })
    responseKeys.push(confidencesKey)
  }

  try {
    await Promise.all(writes)
    await updateManifest(c.env.SCANLAKE_BUCKET, manifestEntries)
  } catch (err) {
    console.error('R2 write failed:', err)
    return c.json({ error: 'Storage write failed' }, 500)
  }

  return c.json({ ok: true, keys: responseKeys })
}
