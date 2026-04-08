interface CheckRequest {
  limit: number
  windowMs: number
}

interface CheckResponse {
  allowed: boolean
  remaining?: number
  retryAfterMs?: number
}

export class RateLimiter implements DurableObject {
  private state: DurableObjectState

  constructor(state: DurableObjectState) {
    this.state = state
  }

  async fetch(request: Request): Promise<Response> {
    if (request.method !== 'POST') {
      return new Response('Method not allowed', { status: 405 })
    }

    const { limit, windowMs } = (await request.json()) as CheckRequest
    const now = Date.now()
    const cutoff = now - windowMs

    const timestamps: number[] = (await this.state.storage.get('timestamps')) ?? []
    const active = timestamps.filter((t) => t > cutoff)

    if (active.length >= limit) {
      const oldestInWindow = active[0]
      const retryAfterMs = oldestInWindow + windowMs - now

      const res: CheckResponse = { allowed: false, retryAfterMs }
      return Response.json(res)
    }

    active.push(now)
    await this.state.storage.put('timestamps', active)

    const res: CheckResponse = { allowed: true, remaining: limit - active.length }
    return Response.json(res)
  }
}
