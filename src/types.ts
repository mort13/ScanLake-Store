export interface Env {
  SCANLAKE_BUCKET: R2Bucket
  RATE_LIMITER: DurableObjectNamespace
  JWT_SECRET: string
  ALLOWED_ORIGIN: string
  API_KEYS: KVNamespace
}

export interface JwtPayload {
  sub: string
  sessionId: string
  iat: number
  exp: number
}

export interface ApiKeyMeta {
  /** The userId this key writes data under in R2. */
  ownerId: string
  active: boolean
  /** Max requests allowed in the rate-limit window. */
  rateLimit: number
  /** Rate-limit window in milliseconds (e.g. 3_600_000 for 1 h). */
  rateLimitWindowMs: number
  /** Maximum bytes allowed per individual file in a single upload. */
  maxUploadBytes: number
  /**
   * If non-empty, requests must carry a matching Origin header.
   * Useful when calls originate from a browser context.
   */
  allowedOrigins: string[]
  createdAt: string
}
