import dotenv from "dotenv";
import brokerService from "./services/broker.service";
import uploaderService from "./services/uploader.service";
import logger from "./utils/logger";
import os from "os";
import path from "path";

// Load environment variables
dotenv.config();

const clientId = process.env.CLIENT_ID;
const filePath =
  process.env.FILE_PATH || path.join(os.homedir(), "file_to_download.txt");

if (!clientId) {
  logger.error("CLIENT_ID environment variable is required");
  process.exit(1);
}

async function startClient() {
  try {
    logger.info(`Starting client: ${clientId}`);
    logger.info(`File path: ${filePath}`);

    // Connect to broker
    await brokerService.connect();

    // Subscribe to commands
    await brokerService.subscribeToCommands(clientId, (command) => {
      logger.info(`Received command: ${command.cmd}`, {
        downloadId: command.downloadId,
      });
      uploaderService.handleUploadCommand(command, filePath);
    });

    logger.info(`Client ${clientId} is ready and listening for commands`);
  } catch (error) {
    logger.error("Failed to start client:", error);
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

startClient();
