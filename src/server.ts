import express, { Request, Response } from "express";
import { createClient } from "redis";
import dotenv from "dotenv";

dotenv.config();

const app = express();

// Use environment variable for Redis URL or default to localhost
const REDIS_URL = process.env.REDIS_URL || "redis://localhost:6379";

// SSE endpoint to stream live comments for a video
app.get("/videos/:videoId/comments/stream", async (req: Request, res: Response) => {
  const { videoId } = req.params;
  const channel = `comments:${videoId}`;
  console.log(`Client connected for live comments on video ${videoId}`);

  // Set SSE headers
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  // Create a dedicated Redis subscriber for this connection
  const subscriber = createClient({ url: REDIS_URL });
  subscriber.on("error", (err) => console.error("Redis Subscriber Error:", err));
  await subscriber.connect();

  // Subscribe to the specific channel for comments
  await subscriber.subscribe(channel, (message) => {
    // Send the message via SSE to the client
    res.write(`data: ${message}\n\n`);
  });

  // Clean up when the client disconnects
  req.on("close", async () => {
    console.log(`Client disconnected from video ${videoId}`);
    await subscriber.unsubscribe(channel);
    await subscriber.disconnect();
    res.end();
  });
});

const PORT = process.env.PORT || 3002;
app.listen(PORT, () => {
  console.log(`Live Comment SSE Service running on port ${PORT}`);
});
