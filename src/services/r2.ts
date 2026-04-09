export function buildR2Key(
  type: 'scans' | 'compositions',
  userId: string,
  sessionId: string,
  batchNumber: number,
): string {
  const now = new Date()
  const yyyy = now.getUTCFullYear()
  const mm = String(now.getUTCMonth() + 1).padStart(2, '0')
  const dd = String(now.getUTCDate()).padStart(2, '0')
  const batch = String(batchNumber).padStart(3, '0')
  return `${yyyy}/${mm}/${dd}/${userId}/${sessionId}/${type}_batch${batch}.parquet`
}

export async function writeParquetToR2(
  bucket: R2Bucket,
  key: string,
  data: ArrayBuffer,
): Promise<void> {
  await bucket.put(key, data, {
    httpMetadata: { contentType: 'application/octet-stream' },
  })
}
