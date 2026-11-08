import { beforeEach, describe, expect, it, vi } from 'vitest'
import { checkUploadFormat, parseFormat, validateParquet } from '../src/services/format'
import {
  FORMAT_V2_URL,
  FORMAT_V3,
  FORMAT_V3_URL,
  POSITIONS_SPEC,
  malformedPositionsParquet,
  positionsParquet,
  scansParquet,
  stubFormatFetch,
} from './helpers'

describe('parseFormat', () => {
  it('accepts a format with a positions spec', () => {
    const json = JSON.parse(JSON.stringify(FORMAT_V3))
    const format = parseFormat(json)
    expect(format.version).toBe(3)
    expect(format.files.positions).toEqual(POSITIONS_SPEC)
  })

  it('still rejects file kinds it does not know', () => {
    const json = { version: 3, files: { trajectories: { columns: [] } } }
    expect(() => parseFormat(json)).toThrow(
      'files.trajectories: unknown file kind (expected scans, compositions, confidences, positions)',
    )
  })

  it('checks positions columns like any other kind', () => {
    const json = { version: 3, files: { positions: { columns: [{ name: 'pos_x', type: 'VECTOR3' }] } } }
    expect(() => parseFormat(json)).toThrow('files.positions.pos_x: type must be one of')
  })
})

describe('validateParquet with the positions spec', () => {
  it('passes a positions file written to the spec', () => {
    expect(validateParquet(positionsParquet(), POSITIONS_SPEC)).toEqual([])
  })

  it('lists every deviation of a malformed positions file', () => {
    expect(validateParquet(malformedPositionsParquet(), POSITIONS_SPEC)).toEqual([
      'column "pos_x" is STRING, expected DOUBLE',
      'missing column "confidence"',
    ])
  })
})

describe('checkUploadFormat', () => {
  beforeEach(() => {
    vi.unstubAllGlobals()
    stubFormatFetch()
  })

  it('rejects a malformed positions file when the format has a positions spec', async () => {
    const result = await checkUploadFormat(FORMAT_V3_URL, [
      ['scans', scansParquet()],
      ['positions', malformedPositionsParquet()],
    ])
    expect(result).toEqual({
      error: 'positions: does not match parquet format v3',
      violations: ['column "pos_x" is STRING, expected DOUBLE', 'missing column "confidence"'],
    })
  })

  it('lets positions through unchecked while the format has no positions spec', async () => {
    const result = await checkUploadFormat(FORMAT_V2_URL, [
      ['scans', scansParquet()],
      ['positions', malformedPositionsParquet()],
    ])
    expect(result).toBeNull()
  })
})
