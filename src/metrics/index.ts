// metrics/prometheus.ts
import { register, Counter, Histogram, Gauge, collectDefaultMetrics } from 'prom-client';

// Collect default metrics (CPU, memory, etc.)
collectDefaultMetrics({ register });

// Custom metrics for Comment Streaming Service
export const httpRequestsTotal = new Counter({
  name: 'http_requests_total',
  help: 'Total number of HTTP requests',
  labelNames: ['method', 'route', 'status_code', 'service'],
  registers: [register]
});

export const httpRequestDuration = new Histogram({
  name: 'http_request_duration_seconds',
  help: 'Duration of HTTP requests in seconds',
  labelNames: ['method', 'route', 'service'],
  buckets: [0.1, 0.5, 1, 2, 5],
  registers: [register]
});

export const sseConnectionsActive = new Gauge({
  name: 'sse_connections_active',
  help: 'Number of active SSE connections',
  labelNames: ['video_id'],
  registers: [register]
});

export const sseConnectionsTotal = new Counter({
  name: 'sse_connections_total',
  help: 'Total number of SSE connections established',
  labelNames: ['video_id'],
  registers: [register]
});

export const sseMessagesStreamed = new Counter({
  name: 'sse_messages_streamed_total',
  help: 'Total number of messages streamed via SSE',
  labelNames: ['video_id'],
  registers: [register]
});

export const redisSubscriberMessages = new Counter({
  name: 'redis_subscriber_messages_total',
  help: 'Total number of messages received from Redis subscriber',
  labelNames: ['channel', 'status'],
  registers: [register]
});

export const redisOperationDuration = new Histogram({
  name: 'redis_operation_duration_seconds',
  help: 'Duration of Redis operations in seconds',
  labelNames: ['operation'],
  buckets: [0.001, 0.005, 0.01, 0.05, 0.1],
  registers: [register]
});

export const activeConnections = new Gauge({
  name: 'redis_connections_active',
  help: 'Number of active Redis connections',
  registers: [register]
});

// Middleware to track HTTP requests
export const metricsMiddleware = (req: any, res: any, next: any) => {
  const start = Date.now();
  
  res.on('finish', () => {
    const duration = (Date.now() - start) / 1000;
    const route = req.route ? req.route.path : req.path;
    
    httpRequestsTotal
      .labels(req.method, route, res.statusCode.toString(), 'comment_streaming')
      .inc();
    
    httpRequestDuration
      .labels(req.method, route, 'comment_streaming')
      .observe(duration);
  });
  
  next();
};

// SSE connection tracking helpers
export const trackSSEConnection = (videoId: string) => {
  sseConnectionsTotal.labels(videoId).inc();
  sseConnectionsActive.labels(videoId).inc();
};

export const trackSSEDisconnection = (videoId: string) => {
  sseConnectionsActive.labels(videoId).dec();
};

export const trackSSEMessage = (videoId: string) => {
  sseMessagesStreamed.labels(videoId).inc();
};

// Redis subscriber message tracking
export const trackRedisMessage = (channel: string, status: 'success' | 'error') => {
  redisSubscriberMessages.labels(channel, status).inc();
};

// Export the register to expose metrics
export { register };