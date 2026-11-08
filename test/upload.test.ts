import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest'
import { Hono } from 'hono'
import type { ApiKeyMeta, Env, JwtPayload } from '../src/types'
import { uploadRoute } from '../src/routes/upload'
import { externalUploadRoute } from '../src/routes/external/upload'
import { formatEnforcerMiddleware } from '../src/middleware/formatEnforcer'
import {
  FORMAT_V2_URL,
  FORMAT_V3_URL,
  FakeBucket,
  SESSION_ID,
  USER_ID,
  allowAllRateLimiter,
  compositionsParquet,
  confidencesParquet,
  malformedPositionsParquet,
  positionsParquet,
  scansParquet,
  stubFormatFetch,
} from './helpers'

type TestEnv = {
  Bindings: Env
  Variables: { jwtPayload: JwtPayload; apiKeyMeta: ApiKeyMeta; apiKeyHash: string }
}

const MAX_UPLOAD_BYTES = 1_000_000

const API_KEY_META: ApiKeyMeta = {
  ownerId: USER_ID,
  active: true,
  rateLimit: 1000,
  rateLimitWindowMs: 3_600_000,
  maxUploadBytes: MAX_UPLOAD_BYTES,
  allowedOrigins: [],
  createdAt: '2026-01-01T00:00:00.000Z',
}

/** Both upload routes, with JWT and API-key auth replaced by fixed credentials for USER_ID. */
function testApp() {
  const app = new Hono<TestEnv>()
  app.post(
    '/api/upload',
    async (c, next) => {
      c.set('jwtPayload', { sub: USER_ID, sessionId: SESSION_ID, iat: 0, exp: 0 })
      await next()
    },
    uploadRoute,
  )
  app.post(
    '/api/external/upload',
    async (c, next) => {
      c.set('apiKeyMeta', API_KEY_META)
      c.set('apiKeyHash', 'test-key-hash')
      await next()
    },
    formatEnforcerMiddleware,
    externalUploadRoute,
  )
  return app
}

const BATCH = 7
const DAY = '2026/11/07'
const key = (kind: string) => `${DAY}/${USER_ID}/${SESSION_ID}/${kind}_batch007.parquet`

function batchForm(files: Record<string, ArrayBuffer>, fields: Record<string, string> = {}): FormData {
  const form = new FormData()
  form.set('sessionId', SESSION_ID)
  form.set('batchNumber', String(BATCH))
  for (const [name, value] of Object.entries(fields)) form.set(name, value)
  for (const [name, data] of Object.entries(files)) {
    form.set(name, new File([data], `${name}_batch007.parquet`))
  }
  return form
}

beforeAll(() => {
  vi.useFakeTimers({ toFake: ['Date'] })
  vi.setSystemTime(new Date('2026-11-07T12:00:00Z'))
  stubFormatFetch()
})

afterAll(() => {
  vi.useRealTimers()
  vi.unstubAllGlobals()
})

describe.each(['/api/upload', '/api/external/upload'])('POST %s', (path) => {
  let bucket: FakeBucket

  beforeEach(() => {
    bucket = new FakeBucket()
  })

  function post(form: FormData, formatUrl = FORMAT_V3_URL) {
    const env = {
      SCANLAKE_BUCKET: bucket.asR2(),
      RATE_LIMITER: allowAllRateLimiter,
      FORMAT_URL: formatUrl,
    } as Env
    return testApp().request(path, { method: 'POST', body: form }, env)
  }

  it('stores scans and compositions only, as before, when nothing optional is sent', async () => {
    const res = await post(batchForm({ scans: scansParquet(), compositions: compositionsParquet() }))

    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ ok: true, keys: [key('scans'), key('compositions')] })
    expect(bucket.keys()).toEqual([key('compositions'), key('scans'), 'manifest.json'])
  })

  it('stores confidences without positions exactly as before', async () => {
    const res = await post(
      batchForm({ scans: scansParquet(), compositions: compositionsParquet(), confidences: confidencesParquet() }),
    )

    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({
      ok: true,
      keys: [key('scans'), key('compositions'), key('confidences')],
    })
    expect(bucket.keys()).toEqual([key('compositions'), key('confidences'), key('scans'), 'manifest.json'])
    expect(bucket.manifest().files.map((f) => f.type).sort()).toEqual(['compositions', 'confidences', 'scans'])
  })

  it('stores an optional positions file next to the batch', async () => {
    const positions = positionsParquet()
    const res = await post(
      batchForm({
        scans: scansParquet(),
        compositions: compositionsParquet(),
        confidences: confidencesParquet(),
        positions,
      }),
    )

    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({
      ok: true,
      keys: [key('scans'), key('compositions'), key('confidences'), key('positions')],
    })
    const stored = await bucket.get(key('positions'))
    expect(new Uint8Array(await stored!.arrayBuffer())).toEqual(new Uint8Array(positions))
  })

  it('lists the positions file in the manifest with type positions', async () => {
    await post(batchForm({ scans: scansParquet(), compositions: compositionsParquet(), positions: positionsParquet() }))

    const manifest = bucket.manifest()
    expect(manifest.fileCount).toBe(3)
    expect(manifest.files.find((f) => f.key === key('positions'))).toMatchObject({
      type: 'positions',
      batchNumber: BATCH,
    })
  })

  it('rejects a positions file that does not match the format with 422 and stores nothing', async () => {
    const res = await post(
      batchForm({ scans: scansParquet(), compositions: compositionsParquet(), positions: malformedPositionsParquet() }),
    )

    expect(res.status).toBe(422)
    expect(await res.json()).toEqual({
      error: 'positions: does not match parquet format v3',
      violations: ['column "pos_x" is STRING, expected DOUBLE', 'missing column "confidence"'],
    })
    expect(bucket.keys()).toEqual([])
  })

  it('rejects a positions file that is not parquet with 422 and stores nothing', async () => {
    const notParquet = new TextEncoder().encode('Zone: OOC_Stanton_1_Hurston').buffer as ArrayBuffer
    const res = await post(
      batchForm({ scans: scansParquet(), compositions: compositionsParquet(), positions: notParquet }),
    )

    expect(res.status).toBe(422)
    expect((await res.json()).error).toMatch(/^positions: /)
    expect(bucket.keys()).toEqual([])
  })

  it('stores positions unchecked while the published format has no positions spec', async () => {
    const res = await post(
      batchForm({ scans: scansParquet(), compositions: compositionsParquet(), positions: malformedPositionsParquet() }),
      FORMAT_V2_URL,
    )

    expect(res.status).toBe(200)
    expect(bucket.keys()).toContain(key('positions'))
  })

  it('ignores form fields it does not know', async () => {
    const res = await post(
      batchForm(
        { scans: scansParquet(), compositions: compositionsParquet(), trajectories: positionsParquet() },
        { isFinal: 'true', note: 'hello' },
      ),
    )

    expect(res.status).toBe(200)
    expect(await res.json()).toEqual({ ok: true, keys: [key('scans'), key('compositions')] })
    expect(bucket.keys()).toEqual([key('compositions'), key('scans'), 'manifest.json'])
  })
})

describe('external upload size budget', () => {
  function postWithLength(contentLength: number) {
    const env = { SCANLAKE_BUCKET: new FakeBucket().asR2(), RATE_LIMITER: allowAllRateLimiter } as Env
    const app = new Hono<TestEnv>()
    app.post(
      '/upload',
      async (c, next) => {
        c.set('apiKeyMeta', API_KEY_META)
        await next()
      },
      formatEnforcerMiddleware,
      (c) => c.text('passed'),
    )
    return app.request(
      '/upload',
      {
        method: 'POST',
        headers: {
          'Content-Type': 'multipart/form-data; boundary=x',
          'Content-Length': String(contentLength),
        },
        body: '',
      },
      env,
    )
  }

  it('lets four full-size files plus form fields through', async () => {
    const res = await postWithLength(MAX_UPLOAD_BYTES * 4 + 4096)
    expect(res.status).toBe(200)
  })

  it('rejects a body over the four-file budget with 413', async () => {
    const res = await postWithLength(MAX_UPLOAD_BYTES * 4 + 4097)
    expect(res.status).toBe(413)
  })
})
