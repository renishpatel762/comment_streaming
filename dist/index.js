"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
const express_1 = __importDefault(require("express"));
const redis_1 = require("redis");
const cors_1 = __importDefault(require("cors"));
const metrics_1 = require("./metrics");
const lokiLogger_1 = require("./logs/lokiLogger");
const os_1 = __importDefault(require("os"));
const app = (0, express_1.default)();
// Apply metrics middleware
app.use(metrics_1.metricsMiddleware);
// Use environment variable for Redis URL or default to localhost
const REDIS_URL = process.env.REDIS_URL;
const CHANNEL = `live-comments`;
// Global mapping: videoId => array of SSE responses
const connections = new Map();
function startSubscriber() {
    return __awaiter(this, void 0, void 0, function* () {
        const subscriber = (0, redis_1.createClient)({
            url: REDIS_URL
        });
        subscriber.on("error", (err) => {
            console.error("Redis Error:", err);
            lokiLogger_1.logger.error("Redis subscriber error", {
                error: err.message,
                redisUrl: REDIS_URL,
                channel: CHANNEL
            });
            (0, metrics_1.trackRedisMessage)(CHANNEL, 'error');
        });
        subscriber.on("connect", () => {
            metrics_1.activeConnections.inc();
            console.log("Redis subscriber connected.");
            lokiLogger_1.logger.info("Redis subscriber connected", {
                redisUrl: REDIS_URL,
                channel: CHANNEL
            });
        });
        subscriber.on("end", () => {
            metrics_1.activeConnections.dec();
            console.log("Redis subscriber disconnected.");
            lokiLogger_1.logger.warn("Redis subscriber disconnected", {
                redisUrl: REDIS_URL,
                channel: CHANNEL
            });
        });
        yield subscriber.connect();
        lokiLogger_1.logger.info("Redis subscriber service started", {
            channel: CHANNEL,
            hostname: os_1.default.hostname()
        });
        // Subscribe to the CHANNEL
        yield subscriber.subscribe(CHANNEL, (message) => {
            try {
                const msg = JSON.parse(message);
                const { videoId } = msg;
                console.log(`[${new Date().toISOString()}] Received message for video ${videoId}: ${message}`);
                lokiLogger_1.logger.info("Redis message received", {
                    videoId,
                    commentId: msg.id,
                    author: msg.author,
                    messageSize: message.length
                });
                (0, metrics_1.trackRedisMessage)(CHANNEL, 'success');
                if (connections.has(videoId)) {
                    const clients = connections.get(videoId);
                    let successfulStreams = 0;
                    // Clean up dead connections and count successful streams
                    const activeClients = clients.filter(client => {
                        try {
                            client.write(`data: ${JSON.stringify(msg)}\n\n`);
                            successfulStreams++;
                            return true;
                        }
                        catch (err) {
                            console.log("Dead connection removed");
                            lokiLogger_1.logger.warn("Dead SSE connection removed", {
                                videoId,
                                error: err instanceof Error ? err.message : 'Unknown error'
                            });
                            return false;
                        }
                    });
                    // Update connections map with only active clients
                    connections.set(videoId, activeClients);
                    // Track successful message streaming
                    if (successfulStreams > 0) {
                        (0, metrics_1.trackSSEMessage)(videoId);
                        lokiLogger_1.logger.info("Message streamed to clients", {
                            videoId,
                            clientCount: successfulStreams,
                            commentId: msg.id
                        });
                    }
                    console.log(`Streamed to ${successfulStreams} clients for video ${videoId}`);
                }
                else {
                    lokiLogger_1.logger.warn("No active connections for video", { videoId });
                }
            }
            catch (err) {
                console.error("Error parsing message:", err);
                lokiLogger_1.logger.error("Failed to parse Redis message", {
                    error: err instanceof Error ? err.message : 'Unknown error',
                    channel: CHANNEL,
                    messagePreview: message.substring(0, 100)
                });
                (0, metrics_1.trackRedisMessage)(CHANNEL, 'error');
            }
        });
    });
}
app.use((0, cors_1.default)());
// SSE endpoint to stream live comments for a video
app.get("/videos/:videoId/comments/stream", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    var _a;
    const { videoId } = req.params;
    const clientIp = req.ip || req.socket.remoteAddress;
    console.log("Connection started for videoId", videoId);
    lokiLogger_1.logger.info("SSE connection request", {
        videoId,
        clientIp,
        userAgent: (_a = req.get('User-Agent')) === null || _a === void 0 ? void 0 : _a.substring(0, 100)
    });
    // Set SSE headers
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.flushHeaders();
    // Track new SSE connection
    (0, metrics_1.trackSSEConnection)(videoId);
    if (!connections.has(videoId)) {
        connections.set(videoId, []);
    }
    connections.get(videoId).push(res);
    const totalConnections = connections.get(videoId).length;
    console.log(`Client connected for video ${videoId}. Total connections: ${totalConnections}`);
    lokiLogger_1.logger.info("SSE connection established", {
        videoId,
        totalConnectionsForVideo: totalConnections,
        totalActiveVideos: connections.size,
        clientIp
    });
    // Send initial connection confirmation
    res.write(`data: ${JSON.stringify({
        type: 'connection',
        message: 'Connected to live comments stream',
        videoId,
        timestamp: new Date().toISOString()
    })}\n\n`);
    // Clean up when the client disconnects
    req.on("close", () => {
        console.log(`Client disconnected from video ${videoId}`);
        // Remove this connection from the map
        if (connections.has(videoId)) {
            const clients = connections.get(videoId);
            const index = clients.indexOf(res);
            if (index > -1) {
                clients.splice(index, 1);
                (0, metrics_1.trackSSEDisconnection)(videoId);
                lokiLogger_1.logger.info("SSE connection closed", {
                    videoId,
                    remainingConnectionsForVideo: clients.length,
                    totalActiveVideos: connections.size,
                    clientIp
                });
            }
            // Clean up empty arrays
            if (clients.length === 0) {
                connections.delete(videoId);
                lokiLogger_1.logger.info("All connections closed for video", { videoId });
            }
        }
        res.end();
    });
    // Handle client errors
    res.on('error', (err) => {
        console.error(`SSE connection error for video ${videoId}:`, err);
        lokiLogger_1.logger.error("SSE connection error", {
            videoId,
            error: err.message,
            clientIp
        });
        (0, metrics_1.trackSSEDisconnection)(videoId);
    });
}));
// Health check endpoint
app.get('/health', (req, res) => {
    const totalConnections = Array.from(connections.values())
        .reduce((sum, clients) => sum + clients.length, 0);
    const healthData = {
        status: 'healthy',
        service: 'comment_streaming',
        activeConnections: totalConnections,
        activeVideos: connections.size,
        timestamp: new Date().toISOString(),
        hostname: os_1.default.hostname(),
        uptime: process.uptime()
    };
    lokiLogger_1.logger.info("Health check requested", {
        activeConnections: totalConnections,
        activeVideos: connections.size,
        uptime: process.uptime()
    });
    res.status(200).json(healthData);
});
// Metrics endpoint for Prometheus
app.get('/metrics', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const metrics = yield metrics_1.register.metrics();
        res.set('Content-Type', metrics_1.register.contentType);
        res.send(metrics);
        lokiLogger_1.logger.debug("Metrics endpoint accessed");
    }
    catch (error) {
        console.error('Error generating metrics:', error);
        lokiLogger_1.logger.error("Failed to generate metrics", {
            error: error instanceof Error ? error.message : 'Unknown error'
        });
        res.status(500).send('Error generating metrics');
    }
}));
// Debug endpoint to see active connections
app.get('/debug/connections', (req, res) => {
    const connectionInfo = Array.from(connections.entries()).map(([videoId, clients]) => ({
        videoId,
        clientCount: clients.length
    }));
    const debugData = {
        totalVideos: connections.size,
        connections: connectionInfo,
        timestamp: new Date().toISOString(),
        hostname: os_1.default.hostname()
    };
    lokiLogger_1.logger.info("Debug endpoint accessed", {
        totalVideos: connections.size,
        requestIp: req.ip
    });
    res.json(debugData);
});
// Start subscriber
startSubscriber().catch((err) => {
    console.error("Failed to start subscriber:", err);
    lokiLogger_1.logger.error("Failed to start Redis subscriber", {
        error: err.message,
        redisUrl: REDIS_URL
    });
});
// Graceful shutdown
process.on('SIGTERM', () => __awaiter(void 0, void 0, void 0, function* () {
    console.log('SIGTERM received, shutting down gracefully');
    lokiLogger_1.logger.info("Graceful shutdown initiated", {
        activeConnections: Array.from(connections.values()).reduce((sum, clients) => sum + clients.length, 0),
        activeVideos: connections.size
    });
    // Close all SSE connections
    for (const [videoId, clients] of connections) {
        clients.forEach(client => {
            try {
                client.end();
            }
            catch (err) {
                console.error('Error closing SSE connection:', err);
                lokiLogger_1.logger.error("Error closing SSE connection during shutdown", {
                    videoId,
                    error: err instanceof Error ? err.message : 'Unknown error'
                });
            }
        });
    }
    lokiLogger_1.logger.info("Service shutdown complete");
    process.exit(0);
}));
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
    console.log(`Live Comment SSE Service running on port ${PORT}`);
    lokiLogger_1.logger.info("Comment streaming service started", {
        port: PORT,
        hostname: os_1.default.hostname(),
        nodeVersion: process.version,
        environment: process.env.NODE_ENV || 'development'
    });
});
