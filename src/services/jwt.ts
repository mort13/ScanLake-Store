import type { JwtPayload } from '../types'

const encoder = new TextEncoder()

function base64UrlEncode(buf: ArrayBuffer | Uint8Array): string {
  const bytes = buf instanceof Uint8Array ? buf : new Uint8Array(buf)
  let binary = ''
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i])
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

function base64UrlDecode(str: string): Uint8Array {
  const padded = str.replace(/-/g, '+').replace(/_/g, '/') + '=='.slice(0, (4 - (str.length % 4)) % 4)
  const binary = atob(padded)
  const bytes = new Uint8Array(binary.length)
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i)
  }
  return bytes
}

async function importKey(secret: string, usage: 'sign' | 'verify'): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    'raw',
    encoder.encode(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    [usage],
  )
}

const HEADER_B64 = base64UrlEncode(encoder.encode(JSON.stringify({ alg: 'HS256', typ: 'JWT' })))

export async function signToken(payload: JwtPayload, secret: string): Promise<string> {
  const payloadB64 = base64UrlEncode(encoder.encode(JSON.stringify(payload)))
  const data = encoder.encode(`${HEADER_B64}.${payloadB64}`)
  const key = await importKey(secret, 'sign')
  const sig = await crypto.subtle.sign('HMAC', key, data)
  return `${HEADER_B64}.${payloadB64}.${base64UrlEncode(sig)}`
}

export async function verifyToken(token: string, secret: string): Promise<JwtPayload> {
  const parts = token.split('.')
  if (parts.length !== 3) throw new Error('Invalid token format')

  const key = await importKey(secret, 'verify')
  const data = encoder.encode(`${parts[0]}.${parts[1]}`)
  const sig = base64UrlDecode(parts[2])

  const valid = await crypto.subtle.verify('HMAC', key, sig.buffer as ArrayBuffer, data)
  if (!valid) throw new Error('Invalid token signature')

  const payload: JwtPayload = JSON.parse(new TextDecoder().decode(base64UrlDecode(parts[1])))

  if (payload.exp <= Date.now() / 1000) {
    throw new Error('Token expired')
  }

  return payload
}
