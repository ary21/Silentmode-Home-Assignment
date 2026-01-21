import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import storageService from "./services/storage.service";
import brokerService from "./services/broker.service";
import downloadService from "./services/download.service";
import { authMiddleware } from "./middleware/auth.middleware";
import * as downloadController from "./controllers/download.controller";
import logger from "./utils/logger";

// Load environment variables
dotenv.config();

const app = express();
const port = parseInt(process.env.SERVER_PORT || "8080", 10);

// Middleware
app.use(cors());
app.use(express.json());

// Public routes
app.get("/health", downloadController.healthCheck);

// Protected routes
app.post(
  "/download/:clientId",
  authMiddleware,
  downloadController.triggerDownload,
);
app.get(
  "/downloads/:downloadId",
  authMiddleware,
  downloadController.getDownloadStatus,
);
app.get("/clients", authMiddleware, downloadController.listClients);
app.get(
  "/download/:downloadId/artifacts",
  authMiddleware,
  downloadController.getArtifact,
);

async function startServer() {
  try {
    // Initialize services
    logger.info("Initializing services...");

    await storageService.ensureBucket();
    await brokerService.connect();

    // Subscribe to upload events
    await brokerService.subscribeToEvents((event) => {
      downloadService.handleUploadComplete(event);
    });

    // Start HTTP server
    app.listen(port, "0.0.0.0", () => {
      logger.info(`Server listening on port ${port}`);
      logger.info("Server ready to accept requests");
    });
  } catch (error) {
    logger.error("Failed to start server:", error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on("SIGTERM", async () => {
  logger.info("SIGTERM received, shutting down gracefully...");
  await brokerService.disconnect();
  process.exit(0);
});

process.on("SIGINT", async () => {
  logger.info("SIGINT received, shutting down gracefully...");
  await brokerService.disconnect();
  process.exit(0);
});

startServer();
