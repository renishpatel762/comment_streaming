import dotenv from "dotenv";
dotenv.config();
import express, { Request, Response } from "express";
import { createClient } from "redis";
import { IComment } from "./interfaces";


const app = express();

// Use environment variable for Redis URL or default to localhost
const REDIS_HOST = process.env.REDIS_HOST;
const REDIS_PORT = process.env.REDIS_PORT || "6379";
const CHANNEL = `live-comments`;

// Global mapping: videoId => array of SSE responses
// TODO: change to Map ?
const connections: Record<string, Response[]> = {};

async function startSubscriber() {
  const subscriber = createClient({
    socket: {
      host: REDIS_HOST,
      port: parseInt(REDIS_PORT),
    },
  });

  subscriber.on("error", (err) => console.error("Redis Error:", err));

  await subscriber.connect();
  console.log("Redis subscriber connected.");

  // Subscribe to the CHANNEL
  await subscriber.subscribe(CHANNEL, (message) => {
    try {
      const msg: IComment = JSON.parse(message); // Parse message from Redis
      const { videoId } = msg;

      console.log(`Received message for video ${videoId}:`, msg);

      if (connections[videoId]) {
        connections[videoId].forEach((client) => {
          client.write(`data: ${JSON.stringify(msg)}\n\n`); // Ensure JSON format
        });
      }
    } catch (err) {
      console.error("Error parsing message:", err);
    }
  });
}

//TODO: handle letter
//       await subscriber.disconnect();

// SSE endpoint to stream live comments for a video
app.get(
  "/videos/:videoId/comments/stream",
  async (req: Request, res: Response) => {
    const { videoId } = req.params;

    // Set SSE headers
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders();

    // Add the response object to the mapping for this videoId
    if (!connections[videoId]) {
      connections[videoId] = [];
    }
    connections[videoId].push(res);

    // Clean up when the client disconnects
    req.on("close", async () => {
      console.log(`Client disconnected from video ${videoId}`);
      res.end();
    });
  }
);

// start Subsriber
startSubscriber().catch(console.error);

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
  console.log(`Live Comment SSE Service running on port ${PORT}`);
});
