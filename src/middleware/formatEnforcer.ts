import type { MiddlewareHandler } from 'hono'
import type { Env, ApiKeyMeta } from '../types'

/** Hard cap applied regardless of the per-key setting (100 MB). */
const GLOBAL_MAX_REQUEST_BYTES = 100 * 1024 * 1024

type FormatEnv = {
  Bindings: Env
  Variables: { apiKeyMeta: ApiKeyMeta; apiKeyHash: string }
}

/**
 * Early-exit format and size guard for external upload requests.
 *
 * Checks performed before the body is fully parsed:
 *   1. Content-Type must be multipart/form-data.
 *   2. If Content-Length is present it is compared against both the
 *      per-key budget (apiKeyMeta.maxUploadBytes × 3 for up to three
 *      files) and the global hard cap.
 *
 * Deeper validation (required fields, file extensions, magic bytes,
 * per-file size) is performed inside the route handler once the body
 * has been parsed.
 */
export const formatEnforcerMiddleware: MiddlewareHandler<FormatEnv> = async (c, next) => {
  const contentType = c.req.header('Content-Type') ?? ''
  if (!contentType.includes('multipart/form-data')) {
    return c.json({ error: 'Content-Type must be multipart/form-data' }, 415)
  }

  const rawLength = c.req.header('Content-Length')
  if (rawLength !== undefined) {
    const contentLength = parseInt(rawLength, 10)
    if (!isNaN(contentLength)) {
      const meta = c.get('apiKeyMeta')
      // Allow up to 3 files (scans, compositions, confidences) plus form-field overhead.
      const perKeyBudget = meta ? meta.maxUploadBytes * 3 + 4096 : GLOBAL_MAX_REQUEST_BYTES
      const limit = Math.min(perKeyBudget, GLOBAL_MAX_REQUEST_BYTES)
      if (contentLength > limit) {
        return c.json(
          { error: `Request body too large. Maximum allowed: ${limit} bytes` },
          413,
        )
      }
    }
  }

  await next()
}
