import { describe, expect, it } from 'vitest'
import { buildManifest } from '../src/services/manifest'
import { buildR2Key } from '../src/services/r2'
import { FakeBucket, SESSION_ID, USER_ID } from './helpers'

describe('buildManifest', () => {
  it('lists a positions file with type positions', async () => {
    const bucket = new FakeBucket()
    const keys = {
      scans: buildR2Key('scans', USER_ID, SESSION_ID, 12),
      compositions: buildR2Key('compositions', USER_ID, SESSION_ID, 12),
      confidences: buildR2Key('confidences', USER_ID, SESSION_ID, 12),
      positions: buildR2Key('positions', USER_ID, SESSION_ID, 12),
    }
    for (const key of Object.values(keys)) await bucket.put(key, new ArrayBuffer(8))
    await bucket.put('manifest.json', '{}')

    const manifest = await buildManifest(bucket.asR2())

    expect(manifest.fileCount).toBe(4)
    const byKey = new Map(manifest.files.map((f) => [f.key, f]))
    for (const [type, key] of Object.entries(keys)) {
      expect(byKey.get(key)).toMatchObject({ type, userId: USER_ID, sessionId: SESSION_ID, batchNumber: 12 })
    }
  })
})

describe('buildR2Key', () => {
  it('names positions files like the other kinds', () => {
    expect(buildR2Key('positions', USER_ID, SESSION_ID, 7)).toMatch(
      new RegExp(`^\\d{4}/\\d{2}/\\d{2}/${USER_ID}/${SESSION_ID}/positions_batch007\\.parquet$`),
    )
  })
})
