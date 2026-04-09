export interface ParquetFile {
  key: string
  type: 'scans' | 'compositions'
  userId: string
  sessionId: string
  batchNumber: number
  uploadedAt: string
}

export interface ManifestData {
  version: '1.0'
  generatedAt: string
  fileCount: number
  files: ParquetFile[]
}

const MANIFEST_KEY = 'manifest.json'

/**
 * Builds a manifest by listing all parquet files from R2
 */
export async function buildManifest(bucket: R2Bucket): Promise<ManifestData> {
  const files: ParquetFile[] = []

  // List all objects in the bucket
  let listResult = await bucket.list({ prefix: 'scans/' })
  files.push(...parseFilesFromListResult(listResult.objects, 'scans'))

  // Continue listing if truncated
  while (listResult.truncated) {
    listResult = await bucket.list({
      prefix: 'scans/',
      cursor: listResult.cursor,
    })
    files.push(...parseFilesFromListResult(listResult.objects, 'scans'))
  }

  // List compositions as well
  listResult = await bucket.list({ prefix: 'compositions/' })
  files.push(...parseFilesFromListResult(listResult.objects, 'compositions'))

  while (listResult.truncated) {
    listResult = await bucket.list({
      prefix: 'compositions/',
      cursor: listResult.cursor,
    })
    files.push(...parseFilesFromListResult(listResult.objects, 'compositions'))
  }

  // Sort by uploaded time descending
  files.sort((a, b) => new Date(b.uploadedAt).getTime() - new Date(a.uploadedAt).getTime())

  return {
    version: '1.0',
    generatedAt: new Date().toISOString(),
    fileCount: files.length,
    files,
  }
}

/**
 * Parses R2 object list into ParquetFile entries
 * Expected key format: {type}/yyyy/mm/dd/{userId}/{sessionId}/{type}_batch{batchNumber}.parquet
 */
function parseFilesFromListResult(objects: R2Object[], type: 'scans' | 'compositions'): ParquetFile[] {
  return objects
    .filter((obj) => obj.key.endsWith('.parquet'))
    .map((obj) => {
      const parts = obj.key.split('/')
      // Format: type/yyyy/mm/dd/userId/sessionId/filename
      const userId = parts[4] ?? 'unknown'
      const sessionId = parts[5] ?? 'unknown'

      // Extract batch number from filename: scans_batch001.parquet -> 001
      const filename = parts[6] ?? ''
      const batchMatch = filename.match(/_batch(\d+)\.parquet$/)
      const batchNumber = batchMatch ? parseInt(batchMatch[1], 10) : 0

      return {
        key: obj.key,
        type,
        userId,
        sessionId,
        batchNumber,
        uploadedAt: obj.uploaded.toISOString(),
      }
    })
}

/**
 * Saves the manifest to R2
 */
export async function saveManifest(bucket: R2Bucket, manifest: ManifestData): Promise<void> {
  await bucket.put(MANIFEST_KEY, JSON.stringify(manifest, null, 2), {
    httpMetadata: { contentType: 'application/json' },
  })
}

/**
 * Retrieves the manifest from R2
 */
export async function getManifest(bucket: R2Bucket): Promise<ManifestData | null> {
  try {
    const obj = await bucket.get(MANIFEST_KEY)
    if (!obj) return null
    const text = await obj.text()
    return JSON.parse(text) as ManifestData
  } catch {
    return null
  }
}

/**
 * Updates the manifest by adding new files and saving it back
 * This is called after file uploads
 */
export async function updateManifest(
  bucket: R2Bucket,
  newFiles: Array<{ key: string; type: 'scans' | 'compositions' }>,
): Promise<ManifestData> {
  const manifest = await buildManifest(bucket)
  await saveManifest(bucket, manifest)
  return manifest
}
