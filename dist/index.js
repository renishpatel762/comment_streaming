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
const app = (0, express_1.default)();
// Use environment variable for Redis URL or default to localhost
const REDIS_URL = process.env.REDIS_URL;
const CHANNEL = `live-comments`;
// Global mapping: videoId => array of SSE responses
// TODO: change to Map ?
const connections = new Map();
function startSubscriber() {
    return __awaiter(this, void 0, void 0, function* () {
        const subscriber = (0, redis_1.createClient)({
            url: REDIS_URL
        });
        subscriber.on("error", (err) => console.error("Redis Error:", err));
        yield subscriber.connect();
        console.log("Redis subscriber connected.");
        // Subscribe to the CHANNEL
        yield subscriber.subscribe(CHANNEL, (message) => {
            try {
                const msg = JSON.parse(message); // Parse message from Redis
                const { videoId } = msg;
                console.log(`[${new Date().toISOString()}] Received message for video ${videoId}: ${message}`);
                if (connections.has(videoId)) {
                    const clients = connections.get(videoId); // `!` asserts it's not undefined
                    clients.forEach((client) => {
                        client.write(`data: ${JSON.stringify(msg)}\n\n`); // Ensure JSON format
                    });
                }
            }
            catch (err) {
                console.error("Error parsing message:", err);
            }
        });
    });
}
//TODO: handle letter
//       await subscriber.disconnect();
app.use((0, cors_1.default)());
// SSE endpoint to stream live comments for a video
app.get("/videos/:videoId/comments/stream", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const { videoId } = req.params;
    console.log("connection started for videoId", videoId);
    // Set SSE headers
    res.setHeader("Content-Type", "text/event-stream");
    res.setHeader("Cache-Control", "no-cache");
    res.setHeader("Connection", "keep-alive");
    res.flushHeaders();
    if (!connections.has(videoId)) {
        connections.set(videoId, []);
    }
    connections.get(videoId).push(res);
    console.log(`Client connected from video ${videoId}`);
    // Clean up when the client disconnects
    req.on("close", () => __awaiter(void 0, void 0, void 0, function* () {
        console.log(`Client disconnected from video ${videoId}`);
        res.end();
    }));
}));
// start Subsriber
startSubscriber().catch(console.error);
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
    console.log(`Live Comment SSE Service running on port ${PORT}`);
});
