import dotenv from "dotenv";
dotenv.config();
import express, { Request, Response } from "express";
import { createClient } from "redis";
import cors from "cors";
import { IComment } from "./interfaces";


const app = express();

// Use environment variable for Redis URL or default to localhost
const REDIS_URL = process.env.REDIS_URL;
const CHANNEL = `live-comments`;

// Global mapping: videoId => array of SSE responses
// TODO: change to Map ?
const connections= new Map<string, Response[]>();

async function startSubscriber() {
  const subscriber = createClient({
    url: REDIS_URL
  });

  subscriber.on("error", (err) => console.error("Redis Error:", err));

  await subscriber.connect();
  console.log("Redis subscriber connected.");

  // Subscribe to the CHANNEL
  await subscriber.subscribe(CHANNEL, (message) => {
    try {
      const msg: IComment = JSON.parse(message); // Parse message from Redis
      const { videoId } = msg;
      console.log(`[${new Date().toISOString()}] Received message for video ${videoId}: ${message}`);

      if (connections.has(videoId)) {
        const clients = connections.get(videoId)!; // `!` asserts it's not undefined
        clients.forEach((client) => {
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

app.use(cors());

// SSE endpoint to stream live comments for a video
app.get(
  "/videos/:videoId/comments/stream",
  async (req: Request, res: Response) => {
    const { videoId } = req.params;
    console.log("connection started for videoId",videoId);

    // Set SSE headers
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders();

    if (!connections.has(videoId)) {
      connections.set(videoId, []);
    }
    connections.get(videoId)!.push(res);

    console.log(`Client connected from video ${videoId}`);
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
