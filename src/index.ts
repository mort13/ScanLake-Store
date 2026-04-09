import { Hono } from 'hono'
import type { Env, JwtPayload } from './types'
import { corsMiddleware } from './middleware/cors'
import { authMiddleware } from './middleware/auth'
import { sessionRoute } from './routes/session'
import { uploadRoute } from './routes/upload'
import { manifestRoute } from './routes/manifest'

export { RateLimiter } from './durable-objects/RateLimiter'

type AppEnv = { Bindings: Env; Variables: { jwtPayload: JwtPayload } }

const app = new Hono<AppEnv>()

app.use('*', corsMiddleware)

app.post('/api/session', sessionRoute)
app.post('/api/upload', authMiddleware, uploadRoute)
app.get('/api/manifest', manifestRoute)

app.all('*', (c) => c.json({ error: 'Not found' }, 404))

export default app
