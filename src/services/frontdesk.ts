import type { Env, UploadReport } from '../types'

/**
 * Tells ScanLake FrontDesk that a batch was stored, so the landing page can show it
 * landing in the lake. Best effort: an upload never fails because of it.
 */
export async function notifyUpload(env: Env, report: UploadReport): Promise<void> {
  if (!env.FRONTDESK) return
  try {
    await env.FRONTDESK.upload(report)
  } catch (err) {
    console.warn('FrontDesk notification failed:', err)
  }
}
