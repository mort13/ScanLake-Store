import { vi } from 'vitest'
import { parquetWriteBuffer } from 'hyparquet-writer'
import type { ParquetFormat } from '../src/services/format'

export const USER_ID = 'user-1'
export const SESSION_ID = '0f8fad5b-d9cb-469f-a165-70867728950e'

/** Format v3 `positions` spec: one row per Zone line of the debug text, plus CamDir. */
export const POSITIONS_SPEC = {
  description: 'Ship position per scan, one row per Zone line of the debug text.',
  allowExtraColumns: false,
  columns: [
    { name: 'capture_id', type: 'STRING' },
    { name: 'zone_index', type: 'INT32' },
    { name: 'zone', type: 'STRING' },
    { name: 'pos_x', type: 'DOUBLE' },
    { name: 'pos_y', type: 'DOUBLE' },
    { name: 'pos_z', type: 'DOUBLE' },
    { name: 'cam_dir_x', type: 'DOUBLE' },
    { name: 'cam_dir_y', type: 'DOUBLE' },
    { name: 'cam_dir_z', type: 'DOUBLE' },
    { name: 'confidence', type: 'DOUBLE' },
  ],
}

/** A cut-down format v2: the three kinds Store knew before positions. */
export const FORMAT_V2: ParquetFormat = {
  version: 2,
  files: {
    scans: {
      columns: [
        { name: 'capture_id', type: 'STRING' },
        { name: 'mass', type: ['INT32', 'DOUBLE'] },
      ],
    },
    compositions: {
      columns: [
        { name: 'capture_id', type: 'STRING' },
        { name: 'amount', type: 'DOUBLE' },
      ],
    },
    confidences: {
      columns: [
        { name: 'capture_id', type: 'STRING' },
        { name: 'mass_conf', type: 'DOUBLE' },
      ],
    },
  },
}

export const FORMAT_V3: ParquetFormat = {
  version: 3,
  files: { ...FORMAT_V2.files, positions: POSITIONS_SPEC },
}

export const FORMAT_V2_URL = 'https://format.test/v2/format.json'
export const FORMAT_V3_URL = 'https://format.test/v3/format.json'

/** Serves FORMAT_V2 and FORMAT_V3 to loadFormat in place of GitHub. */
export function stubFormatFetch(): void {
  const formats: Record<string, ParquetFormat> = {
    [FORMAT_V2_URL]: FORMAT_V2,
    [FORMAT_V3_URL]: FORMAT_V3,
  }
  vi.stubGlobal('fetch', async (input: RequestInfo | URL) => {
    const format = formats[String(input)]
    return format ? Response.json(format) : new Response('not found', { status: 404 })
  })
}

const CAPTURE_IDS = ['3b241101-e2bb-4255-8caf-4136c566a962', '9c5b94b1-35ad-49bb-b118-8e8fc24abf80']

export function scansParquet(): ArrayBuffer {
  return parquetWriteBuffer({
    columnData: [
      { name: 'capture_id', data: CAPTURE_IDS, type: 'STRING' },
      { name: 'mass', data: Int32Array.from([5120, 830]), type: 'INT32' },
    ],
  })
}

export function compositionsParquet(): ArrayBuffer {
  return parquetWriteBuffer({
    columnData: [
      { name: 'capture_id', data: CAPTURE_IDS, type: 'STRING' },
      { name: 'amount', data: Float64Array.from([41.5, 12.25]), type: 'DOUBLE' },
    ],
  })
}

export function confidencesParquet(): ArrayBuffer {
  return parquetWriteBuffer({
    columnData: [
      { name: 'capture_id', data: CAPTURE_IDS, type: 'STRING' },
      { name: 'mass_conf', data: Float64Array.from([0.98, 0.91]), type: 'DOUBLE' },
    ],
  })
}

/** Two Zone lines for the first scan, one for the second. */
export function positionsParquet(): ArrayBuffer {
  return parquetWriteBuffer({
    columnData: [
      { name: 'capture_id', data: [CAPTURE_IDS[0], CAPTURE_IDS[0], CAPTURE_IDS[1]], type: 'STRING' },
      { name: 'zone_index', data: Int32Array.from([0, 1, 0]), type: 'INT32' },
      { name: 'zone', data: ['OOC_Stanton_1_Hurston', 'Stanton', 'OOC_Stanton_1_Hurston'], type: 'STRING' },
      { name: 'pos_x', data: Float64Array.from([-12873.4, 12850112.9, -9001.25]), type: 'DOUBLE' },
      { name: 'pos_y', data: Float64Array.from([993.06, 1.5e8, 1102.5]), type: 'DOUBLE' },
      { name: 'pos_z', data: Float64Array.from([-6311.2, -2.2e6, -6000.75]), type: 'DOUBLE' },
      { name: 'cam_dir_x', data: Float64Array.from([0.12, 0.12, -0.7]), type: 'DOUBLE' },
      { name: 'cam_dir_y', data: Float64Array.from([-0.98, -0.98, 0.1]), type: 'DOUBLE' },
      { name: 'cam_dir_z', data: Float64Array.from([0.15, 0.15, 0.7]), type: 'DOUBLE' },
      { name: 'confidence', data: Float64Array.from([0.99, 0.99, 0.87]), type: 'DOUBLE' },
    ],
  })
}

/** A readable parquet that breaks the positions spec: pos_x is text and confidence is missing. */
export function malformedPositionsParquet(): ArrayBuffer {
  return parquetWriteBuffer({
    columnData: [
      { name: 'capture_id', data: [CAPTURE_IDS[0]], type: 'STRING' },
      { name: 'zone_index', data: Int32Array.from([0]), type: 'INT32' },
      { name: 'zone', data: ['OOC_Stanton_1_Hurston'], type: 'STRING' },
      { name: 'pos_x', data: ['-12873.4'], type: 'STRING' },
      { name: 'pos_y', data: Float64Array.from([993.06]), type: 'DOUBLE' },
      { name: 'pos_z', data: Float64Array.from([-6311.2]), type: 'DOUBLE' },
      { name: 'cam_dir_x', data: Float64Array.from([0.12]), type: 'DOUBLE' },
      { name: 'cam_dir_y', data: Float64Array.from([-0.98]), type: 'DOUBLE' },
      { name: 'cam_dir_z', data: Float64Array.from([0.15]), type: 'DOUBLE' },
    ],
  })
}

/** In-memory stand-in for the parts of R2Bucket Store uses. */
export class FakeBucket {
  readonly objects = new Map<string, { body: ArrayBuffer | string; uploaded: Date }>()

  async put(key: string, body: ArrayBuffer | string) {
    this.objects.set(key, { body, uploaded: new Date() })
    return {}
  }

  async get(key: string) {
    const obj = this.objects.get(key)
    if (!obj) return null
    const bytes = typeof obj.body === 'string' ? new TextEncoder().encode(obj.body) : new Uint8Array(obj.body)
    return {
      text: async () => new TextDecoder().decode(bytes),
      arrayBuffer: async () => bytes.slice().buffer,
    }
  }

  async list() {
    const objects = [...this.objects].map(([key, obj]) => ({ key, uploaded: obj.uploaded }))
    return { objects, truncated: false }
  }

  keys(): string[] {
    return [...this.objects.keys()].sort()
  }

  manifest(): { fileCount: number; files: Array<{ key: string; type: string; batchNumber: number }> } {
    const obj = this.objects.get('manifest.json')
    if (!obj || typeof obj.body !== 'string') throw new Error('manifest.json not written')
    return JSON.parse(obj.body)
  }

  asR2(): R2Bucket {
    return this as unknown as R2Bucket
  }
}

/** Durable Object namespace whose RateLimiter always allows the request. */
export const allowAllRateLimiter = {
  idFromName: (name: string) => name,
  get: () => ({ fetch: async () => Response.json({ allowed: true }) }),
} as unknown as DurableObjectNamespace
