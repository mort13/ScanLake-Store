import { parquetMetadata, parquetSchema } from 'hyparquet'
import type { SchemaElement } from 'hyparquet'

export type FileKind = 'scans' | 'compositions' | 'confidences'

export interface ColumnSpec {
  name: string
  /** Accepted column type(s), see COLUMN_TYPES. */
  type: string | string[]
  /** Whether the column must be present. Defaults to true. */
  required?: boolean
  description?: string
}

export interface FileSpec {
  description?: string
  /** Whether columns not listed in `columns` are accepted. Defaults to false. */
  allowExtraColumns?: boolean
  columns: ColumnSpec[]
}

/** The format file kept in the ScanLake-Format git repository (format.json). */
export interface ParquetFormat {
  version: number
  files: Partial<Record<FileKind, FileSpec>>
}

/**
 * Type names a column spec may use.  Physical types without annotation keep
 * their Parquet name (BYTE_ARRAY becomes BINARY); annotated columns are named
 * after their logical type, so a UTF8 byte array is STRING and an INT64 with
 * TIMESTAMP_MILLIS is TIMESTAMP.  NESTED covers struct, list and map columns.
 */
const COLUMN_TYPES = new Set([
  'STRING', 'BINARY', 'BOOLEAN',
  'INT8', 'INT16', 'INT32', 'INT64', 'UINT8', 'UINT16', 'UINT32', 'UINT64', 'INT96',
  'FLOAT', 'FLOAT16', 'DOUBLE', 'DECIMAL',
  'DATE', 'TIME', 'TIMESTAMP', 'INTERVAL',
  'JSON', 'BSON', 'UUID', 'ENUM', 'FIXED_LEN_BYTE_ARRAY', 'NESTED',
])

const FILE_KINDS: FileKind[] = ['scans', 'compositions', 'confidences']

/** How long a fetched format is reused before it is fetched again. */
const FORMAT_TTL_MS = 5 * 60_000

/** Upper bound on fetching the format, so a hanging origin cannot stall uploads. */
const FORMAT_FETCH_TIMEOUT_MS = 5_000

let cached: { url: string; format: ParquetFormat; fetchedAt: number } | null = null

/**
 * Returns the parquet format published at `url`, refetched at most every
 * FORMAT_TTL_MS.  If a refetch fails, the last good copy stays in use; if
 * there is none, returns null and the caller skips format validation, so an
 * unreachable or broken format file never blocks uploads.
 */
export async function loadFormat(url: string): Promise<ParquetFormat | null> {
  if (cached && cached.url === url && Date.now() - cached.fetchedAt < FORMAT_TTL_MS) {
    return cached.format
  }
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(FORMAT_FETCH_TIMEOUT_MS) })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const format = parseFormat(await res.json())
    cached = { url, format, fetchedAt: Date.now() }
    return format
  } catch (err) {
    console.error(`Could not load parquet format from ${url}:`, err)
    return cached?.url === url ? cached.format : null
  }
}

/** Checks the structure of a format file, throwing on the first problem. */
export function parseFormat(json: unknown): ParquetFormat {
  if (!isObject(json)) throw new Error('format must be a JSON object')
  if (typeof json.version !== 'number') throw new Error('version must be a number')
  if (!isObject(json.files)) throw new Error('files must be an object')

  for (const [kind, spec] of Object.entries(json.files)) {
    if (!FILE_KINDS.includes(kind as FileKind)) {
      throw new Error(`files.${kind}: unknown file kind (expected ${FILE_KINDS.join(', ')})`)
    }
    if (!isObject(spec) || !Array.isArray(spec.columns)) {
      throw new Error(`files.${kind}.columns must be an array`)
    }
    if (spec.allowExtraColumns !== undefined && typeof spec.allowExtraColumns !== 'boolean') {
      throw new Error(`files.${kind}.allowExtraColumns must be a boolean`)
    }
    const names = new Set<string>()
    for (const col of spec.columns) {
      if (!isObject(col) || typeof col.name !== 'string' || col.name === '') {
        throw new Error(`files.${kind}: every column needs a name`)
      }
      if (names.has(col.name)) throw new Error(`files.${kind}: duplicate column "${col.name}"`)
      names.add(col.name)
      const types = Array.isArray(col.type) ? col.type : [col.type]
      if (types.length === 0 || !types.every((t) => typeof t === 'string' && COLUMN_TYPES.has(t))) {
        throw new Error(
          `files.${kind}.${col.name}: type must be one of ${[...COLUMN_TYPES].join(', ')}`,
        )
      }
      if (col.required !== undefined && typeof col.required !== 'boolean') {
        throw new Error(`files.${kind}.${col.name}: required must be a boolean`)
      }
    }
  }
  return json as unknown as ParquetFormat
}

/** Lists every way the parquet in `data` deviates from `spec` (empty if it matches). */
export function validateParquet(data: ArrayBuffer, spec: FileSpec): string[] {
  let topLevel: SchemaElement[]
  try {
    topLevel = parquetSchema(parquetMetadata(data)).children.map((child) => child.element)
  } catch {
    return ['not a readable Parquet file']
  }

  const actual = new Map(topLevel.map((el) => [el.name, columnType(el)]))
  const violations: string[] = []

  for (const col of spec.columns) {
    const type = actual.get(col.name)
    if (type === undefined) {
      if (col.required !== false) violations.push(`missing column "${col.name}"`)
      continue
    }
    const allowed = Array.isArray(col.type) ? col.type : [col.type]
    if (!allowed.includes(type)) {
      violations.push(`column "${col.name}" is ${type}, expected ${allowed.join(' or ')}`)
    }
  }

  if (!spec.allowExtraColumns) {
    const known = new Set(spec.columns.map((col) => col.name))
    for (const name of actual.keys()) {
      if (!known.has(name)) violations.push(`unexpected column "${name}"`)
    }
  }

  return violations
}

/**
 * Validates every uploaded file against the format at `formatUrl`.
 * Returns an error body for the first file that does not match, or null
 * when all match (or no format could be loaded).
 */
export async function checkUploadFormat(
  formatUrl: string,
  files: Array<[FileKind, ArrayBuffer]>,
): Promise<{ error: string; violations: string[] } | null> {
  const format = await loadFormat(formatUrl)
  if (!format) return null

  for (const [kind, data] of files) {
    const spec = format.files[kind]
    if (!spec) continue
    const violations = validateParquet(data, spec)
    if (violations.length > 0) {
      return { error: `${kind}: does not match parquet format v${format.version}`, violations }
    }
  }
  return null
}

/** Maps a schema element to its COLUMN_TYPES name. */
function columnType(el: SchemaElement): string {
  if (el.num_children || el.repetition_type === 'REPEATED') return 'NESTED'

  const logical = el.logical_type
  if (logical) {
    if (logical.type === 'INTEGER') return `${logical.isSigned ? 'INT' : 'UINT'}${logical.bitWidth}`
    return logical.type
  }

  const converted = el.converted_type
  if (converted) {
    if (converted === 'UTF8') return 'STRING'
    if (converted.startsWith('INT_') || converted.startsWith('UINT_')) return converted.replace('_', '')
    if (converted.startsWith('TIMESTAMP_')) return 'TIMESTAMP'
    if (converted.startsWith('TIME_')) return 'TIME'
    return converted
  }

  return el.type === 'BYTE_ARRAY' ? 'BINARY' : (el.type ?? 'NESTED')
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
