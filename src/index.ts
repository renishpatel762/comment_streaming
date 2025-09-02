import dotenv from "dotenv";
dotenv.config();
import express, { Request, Response } from "express";
import { createClient } from "redis";
import cors from "cors";
import { IComment } from "./interfaces";
import { 
  metricsMiddleware, 
  trackSSEConnection, 
  trackSSEDisconnection, 
  trackSSEMessage,
  trackRedisMessage,
  activeConnections,
  register 
} from "./metrics";
import { logger } from "./logs/lokiLogger";
import os from "os";

const app = express();

// Apply metrics middleware
app.use(metricsMiddleware);

// Use environment variable for Redis URL or default to localhost
const REDIS_URL = process.env.REDIS_URL;
const CHANNEL = `live-comments`;

// Global mapping: videoId => array of SSE responses
const connections = new Map<string, Response[]>();

async function startSubscriber() {
  const subscriber = createClient({
    url: REDIS_URL
  });

  subscriber.on("error", (err) => {
    console.error("Redis Error:", err);
    logger.error("Redis subscriber error", { 
      error: err.message,
      redisUrl: REDIS_URL,
      channel: CHANNEL
    });
    trackRedisMessage(CHANNEL, 'error');
  });

  subscriber.on("connect", () => {
    activeConnections.inc();
    console.log("Redis subscriber connected.");
    logger.info("Redis subscriber connected", {
      redisUrl: REDIS_URL,
      channel: CHANNEL
    });
  });

  subscriber.on("end", () => {
    activeConnections.dec();
    console.log("Redis subscriber disconnected.");
    logger.warn("Redis subscriber disconnected", {
      redisUrl: REDIS_URL,
      channel: CHANNEL
    });
  });

  await subscriber.connect();
  logger.info("Redis subscriber service started", { 
    channel: CHANNEL,
    hostname: os.hostname() 
  });

  // Subscribe to the CHANNEL
  await subscriber.subscribe(CHANNEL, (message) => {
    try {
      const msg: IComment = JSON.parse(message);
      const { videoId } = msg;
      
      console.log(`[${new Date().toISOString()}] Received message for video ${videoId}: ${message}`);
      logger.info("Redis message received", {
        videoId,
        commentId: msg.id,
        author: msg.author,
        messageSize: message.length
      });

      trackRedisMessage(CHANNEL, 'success');

      if (connections.has(videoId)) {
        const clients = connections.get(videoId)!;
        let successfulStreams = 0;
        
        // Clean up dead connections and count successful streams
        const activeClients = clients.filter(client => {
          try {
            client.write(`data: ${JSON.stringify(msg)}\n\n`);
            successfulStreams++;
            return true;
          } catch (err) {
            console.log("Dead connection removed");
            logger.warn("Dead SSE connection removed", {
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
          trackSSEMessage(videoId);
          logger.info("Message streamed to clients", {
            videoId,
            clientCount: successfulStreams,
            commentId: msg.id
          });
        }
        
        console.log(`Streamed to ${successfulStreams} clients for video ${videoId}`);
      } else {
        logger.warn("No active connections for video", { videoId });
      }
    } catch (err) {
      console.error("Error parsing message:", err);
      logger.error("Failed to parse Redis message", {
        error: err instanceof Error ? err.message : 'Unknown error',
        channel: CHANNEL,
        messagePreview: message.substring(0, 100)
      });
      trackRedisMessage(CHANNEL, 'error');
    }
  });
}

app.use(cors());

// SSE endpoint to stream live comments for a video
app.get(
  "/videos/:videoId/comments/stream",
  async (req: Request, res: Response) => {
    const { videoId } = req.params;
    const clientIp = req.ip || req.socket.remoteAddress;
    
    console.log("Connection started for videoId", videoId);
    logger.info("SSE connection request", {
      videoId,
      clientIp,
      userAgent: req.get('User-Agent')?.substring(0, 100)
    });

    // Set SSE headers
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.flushHeaders();

    // Track new SSE connection
    trackSSEConnection(videoId);

    if (!connections.has(videoId)) {
      connections.set(videoId, []);
    }
    connections.get(videoId)!.push(res);

    const totalConnections = connections.get(videoId)!.length;
    console.log(`Client connected for video ${videoId}. Total connections: ${totalConnections}`);
    
    logger.info("SSE connection established", {
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
        const clients = connections.get(videoId)!;
        const index = clients.indexOf(res);
        if (index > -1) {
          clients.splice(index, 1);
          trackSSEDisconnection(videoId);
          
          logger.info("SSE connection closed", {
            videoId,
            remainingConnectionsForVideo: clients.length,
            totalActiveVideos: connections.size,
            clientIp
          });
        }
        
        // Clean up empty arrays
        if (clients.length === 0) {
          connections.delete(videoId);
          logger.info("All connections closed for video", { videoId });
        }
      }
      
      res.end();
    });

    // Handle client errors
    res.on('error', (err) => {
      console.error(`SSE connection error for video ${videoId}:`, err);
      logger.error("SSE connection error", {
        videoId,
        error: err.message,
        clientIp
      });
      trackSSEDisconnection(videoId);
    });
  }
);

// Health check endpoint
app.get('/health', (req: Request, res: Response) => {
  const totalConnections = Array.from(connections.values())
    .reduce((sum, clients) => sum + clients.length, 0);
    
  const healthData = {
    status: 'healthy', 
    service: 'comment_streaming',
    activeConnections: totalConnections,
    activeVideos: connections.size,
    timestamp: new Date().toISOString(),
    hostname: os.hostname(),
    uptime: process.uptime()
  };

  logger.info("Health check requested", {
    activeConnections: totalConnections,
    activeVideos: connections.size,
    uptime: process.uptime()
  });
    
  res.status(200).json(healthData);
});

// Metrics endpoint for Prometheus
app.get('/metrics', async (req: Request, res: Response) => {
  try {
    const metrics = await register.metrics();
    res.set('Content-Type', register.contentType);
    res.send(metrics);
    
    logger.debug("Metrics endpoint accessed");
  } catch (error) {
    console.error('Error generating metrics:', error);
    logger.error("Failed to generate metrics", {
      error: error instanceof Error ? error.message : 'Unknown error'
    });
    res.status(500).send('Error generating metrics');
  }
});

// Debug endpoint to see active connections
app.get('/debug/connections', (req: Request, res: Response) => {
  const connectionInfo = Array.from(connections.entries()).map(([videoId, clients]) => ({
    videoId,
    clientCount: clients.length
  }));
  
  const debugData = {
    totalVideos: connections.size,
    connections: connectionInfo,
    timestamp: new Date().toISOString(),
    hostname: os.hostname()
  };

  logger.info("Debug endpoint accessed", {
    totalVideos: connections.size,
    requestIp: req.ip
  });
  
  res.json(debugData);
});

// Start subscriber
startSubscriber().catch((err) => {
  console.error("Failed to start subscriber:", err);
  logger.error("Failed to start Redis subscriber", {
    error: err.message,
    redisUrl: REDIS_URL
  });
});

// Graceful shutdown
process.on('SIGTERM', async () => {
  console.log('SIGTERM received, shutting down gracefully');
  logger.info("Graceful shutdown initiated", {
    activeConnections: Array.from(connections.values()).reduce((sum, clients) => sum + clients.length, 0),
    activeVideos: connections.size
  });
  
  // Close all SSE connections
  for (const [videoId, clients] of connections) {
    clients.forEach(client => {
      try {
        client.end();
      } catch (err) {
        console.error('Error closing SSE connection:', err);
        logger.error("Error closing SSE connection during shutdown", {
          videoId,
          error: err instanceof Error ? err.message : 'Unknown error'
        });
      }
    });
  }
  
  logger.info("Service shutdown complete");
  process.exit(0);
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Live Comment SSE Service running on port ${PORT}`);
  logger.info("Comment streaming service started", {
    port: PORT,
    hostname: os.hostname(),
    nodeVersion: process.version,
    environment: process.env.NODE_ENV || 'development'
  });
});