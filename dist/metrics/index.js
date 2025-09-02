"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.register = exports.trackRedisMessage = exports.trackSSEMessage = exports.trackSSEDisconnection = exports.trackSSEConnection = exports.metricsMiddleware = exports.activeConnections = exports.redisOperationDuration = exports.redisSubscriberMessages = exports.sseMessagesStreamed = exports.sseConnectionsTotal = exports.sseConnectionsActive = exports.httpRequestDuration = exports.httpRequestsTotal = void 0;
// metrics/prometheus.ts
const prom_client_1 = require("prom-client");
Object.defineProperty(exports, "register", { enumerable: true, get: function () { return prom_client_1.register; } });
// Collect default metrics (CPU, memory, etc.)
(0, prom_client_1.collectDefaultMetrics)({ register: prom_client_1.register });
// Custom metrics for Comment Streaming Service
exports.httpRequestsTotal = new prom_client_1.Counter({
    name: 'http_requests_total',
    help: 'Total number of HTTP requests',
    labelNames: ['method', 'route', 'status_code', 'service'],
    registers: [prom_client_1.register]
});
exports.httpRequestDuration = new prom_client_1.Histogram({
    name: 'http_request_duration_seconds',
    help: 'Duration of HTTP requests in seconds',
    labelNames: ['method', 'route', 'service'],
    buckets: [0.1, 0.5, 1, 2, 5],
    registers: [prom_client_1.register]
});
exports.sseConnectionsActive = new prom_client_1.Gauge({
    name: 'sse_connections_active',
    help: 'Number of active SSE connections',
    labelNames: ['video_id'],
    registers: [prom_client_1.register]
});
exports.sseConnectionsTotal = new prom_client_1.Counter({
    name: 'sse_connections_total',
    help: 'Total number of SSE connections established',
    labelNames: ['video_id'],
    registers: [prom_client_1.register]
});
exports.sseMessagesStreamed = new prom_client_1.Counter({
    name: 'sse_messages_streamed_total',
    help: 'Total number of messages streamed via SSE',
    labelNames: ['video_id'],
    registers: [prom_client_1.register]
});
exports.redisSubscriberMessages = new prom_client_1.Counter({
    name: 'redis_subscriber_messages_total',
    help: 'Total number of messages received from Redis subscriber',
    labelNames: ['channel', 'status'],
    registers: [prom_client_1.register]
});
exports.redisOperationDuration = new prom_client_1.Histogram({
    name: 'redis_operation_duration_seconds',
    help: 'Duration of Redis operations in seconds',
    labelNames: ['operation'],
    buckets: [0.001, 0.005, 0.01, 0.05, 0.1],
    registers: [prom_client_1.register]
});
exports.activeConnections = new prom_client_1.Gauge({
    name: 'redis_connections_active',
    help: 'Number of active Redis connections',
    registers: [prom_client_1.register]
});
// Middleware to track HTTP requests
const metricsMiddleware = (req, res, next) => {
    const start = Date.now();
    res.on('finish', () => {
        const duration = (Date.now() - start) / 1000;
        const route = req.route ? req.route.path : req.path;
        exports.httpRequestsTotal
            .labels(req.method, route, res.statusCode.toString(), 'comment_streaming')
            .inc();
        exports.httpRequestDuration
            .labels(req.method, route, 'comment_streaming')
            .observe(duration);
    });
    next();
};
exports.metricsMiddleware = metricsMiddleware;
// SSE connection tracking helpers
const trackSSEConnection = (videoId) => {
    exports.sseConnectionsTotal.labels(videoId).inc();
    exports.sseConnectionsActive.labels(videoId).inc();
};
exports.trackSSEConnection = trackSSEConnection;
const trackSSEDisconnection = (videoId) => {
    exports.sseConnectionsActive.labels(videoId).dec();
};
exports.trackSSEDisconnection = trackSSEDisconnection;
const trackSSEMessage = (videoId) => {
    exports.sseMessagesStreamed.labels(videoId).inc();
};
exports.trackSSEMessage = trackSSEMessage;
// Redis subscriber message tracking
const trackRedisMessage = (channel, status) => {
    exports.redisSubscriberMessages.labels(channel, status).inc();
};
exports.trackRedisMessage = trackRedisMessage;
