export interface Env {
  SCANLAKE_BUCKET: R2Bucket
  RATE_LIMITER: DurableObjectNamespace
  JWT_SECRET: string
  ALLOWED_ORIGIN: string
}

export interface JwtPayload {
  sub: string
  sessionId: string
  iat: number
  exp: number
}
