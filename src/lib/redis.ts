import { Redis, RedisOptions } from "ioredis";
import { URL } from 'url'; // Import URL parser

const redisUrlString = process.env.UPSTASH_REDIS_URL;

if (!redisUrlString) {
  throw new Error("UPSTASH_REDIS_URL environment variable is not set.");
}

const redisUrl = new URL(redisUrlString);

const redisOptions: RedisOptions = {
  host: redisUrl.hostname,
  port: parseInt(redisUrl.port, 10),
  password: redisUrl.password,
  maxRetriesPerRequest: null, // Required by BullMQ
  lazyConnect: true, // Avoid immediate connection errors if parsing fails
};

// Enable TLS if the protocol is rediss://
if (redisUrl.protocol === 'rediss:') {
  redisOptions.tls = {}; // Enable TLS
}

export const connection = new Redis(redisOptions);

// Optional: Add connection listeners for debugging
connection.on('connect', () => console.log('Redis connected successfully.'));
connection.on('error', (err) => console.error('Redis connection error:', err));