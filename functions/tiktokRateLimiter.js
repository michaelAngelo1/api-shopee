import Bottleneck from 'bottleneck';
import Redis from 'ioredis';

// Use your Redis Memorystore connection string
const redis = new Redis(process.env.REDIS_URL);

const limiter = new Bottleneck({
  id: 'tiktok-api-global', // Unique ID for this limiter
  minTime: 1200,           // 1.2 seconds between requests (adjust as needed)
  maxConcurrent: 1,        // Only one request at a time
  datastore: 'ioredis',
  clearDatastore: false,
  clientOptions: { client: redis }
});

redis.on('error', (err) => {
  console.error('[RATE-LIMITER] Redis connection error:', err);
});
redis.on('connect', () => {
  console.log('[RATE-LIMITER] Connected to Redis!');
});

// Export a wrapper for your API calls
export function rateLimitedCall(fn, ...args) {
  return limiter.schedule(() => fn(...args));
}