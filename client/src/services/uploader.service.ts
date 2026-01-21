import fs from "fs";
import crypto from "crypto";
import axios from "axios";
import brokerService from "./broker.service";
import logger from "../utils/logger";

interface UploadCommand {
  cmd: "upload";
  downloadId: string;
  objectKey: string;
  presignedUrl: string;
  expiresAt: string;
  meta?: any;
}

class UploaderService {
  private activeDownloads: Set<string> = new Set();
  private maxRetries: number = 5;
  private baseDelay: number = 1000; // 1 second

  async handleUploadCommand(
    command: UploadCommand,
    filePath: string,
  ): Promise<void> {
    const { downloadId, objectKey, presignedUrl, expiresAt } = command;

    // Validate command
    if (!downloadId || !objectKey || !presignedUrl) {
      logger.error("Invalid command received: missing required fields", {
        downloadId,
      });
      return;
    }

    // Check if already processing
    if (this.activeDownloads.has(downloadId)) {
      logger.warn(
        `Download ${downloadId} is already being processed, ignoring duplicate`,
      );
      return;
    }

    // Check if presigned URL is expired
    const expiresDate = new Date(expiresAt);
    if (expiresDate < new Date()) {
      logger.error(`Presigned URL expired for ${downloadId}`);
      await this.publishFailedEvent(
        downloadId,
        objectKey,
        "Presigned URL expired",
      );
      return;
    }

    // Mark as active
    this.activeDownloads.add(downloadId);

    try {
      logger.info(`Starting upload for ${downloadId}`, { objectKey });

      const result = await this.uploadFileWithRetry(
        presignedUrl,
        filePath,
        downloadId,
      );

      // Publish success event
      await brokerService.publishEvent({
        event: "upload_complete",
        downloadId,
        objectKey,
        size: result.size,
        sha256: result.sha256,
        status: "ok",
        timestamp: new Date().toISOString(),
      });

      logger.info(`Upload successful for ${downloadId}`, {
        size: result.size,
        sha256: result.sha256,
      });
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      logger.error(`Upload failed for ${downloadId}: ${errorMessage}`, {
        stack: error instanceof Error ? error.stack : undefined,
      });
      await this.publishFailedEvent(downloadId, objectKey, errorMessage);
    } finally {
      this.activeDownloads.delete(downloadId);
    }
  }

  private async uploadFileWithRetry(
    presignedUrl: string,
    filePath: string,
    downloadId: string,
  ): Promise<{ size: number; sha256: string }> {
    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= this.maxRetries; attempt++) {
      try {
        logger.debug(
          `Upload attempt ${attempt}/${this.maxRetries} for ${downloadId}`,
        );
        return await this.uploadFile(presignedUrl, filePath);
      } catch (error) {
        lastError = error instanceof Error ? error : new Error("Unknown error");
        logger.warn(
          `Upload attempt ${attempt} failed for ${downloadId}: ${lastError.message}`,
          { stack: lastError.stack },
        );

        if (attempt < this.maxRetries) {
          const delay = this.baseDelay * Math.pow(2, attempt - 1); // Exponential backoff
          logger.info(`Retrying in ${delay}ms...`);
          await this.sleep(delay);
        }
      }
    }

    throw new Error(
      `Upload failed after ${this.maxRetries} attempts: ${lastError?.message}`,
    );
  }

  private async uploadFile(
    presignedUrl: string,
    filePath: string,
  ): Promise<{ size: number; sha256: string }> {
    // Verify file exists
    if (!fs.existsSync(filePath)) {
      throw new Error(`File not found: ${filePath}`);
    }

    // Get file stats
    const stats = fs.statSync(filePath);
    const size = stats.size;

    // Create hash instance
    const hash = crypto.createHash("sha256");

    // Create read stream
    const fileStream = fs.createReadStream(filePath);

    // Update hash as data flows
    fileStream.on("data", (chunk) => {
      hash.update(chunk);
    });

    // Upload with streaming
    logger.debug(
      `Uploading file (${size} bytes) with streaming hash calculation...`,
    );

    await axios.put(presignedUrl, fileStream, {
      headers: {
        "Content-Type": "application/octet-stream",
        "Content-Length": size,
      },
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
    });

    const sha256 = hash.digest("hex");
    logger.debug(`Upload complete. SHA256: ${sha256}`);

    return { size, sha256 };
  }

  private async publishFailedEvent(
    downloadId: string,
    objectKey: string,
    reason: string,
  ): Promise<void> {
    try {
      await brokerService.publishEvent({
        event: "upload_failed",
        downloadId,
        objectKey,
        reason,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      const errorMessage =
        error instanceof Error ? error.message : "Unknown error";
      logger.error(`Error publishing failed event: ${errorMessage}`, {
        stack: error instanceof Error ? error.stack : undefined,
      });
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

export default new UploaderService();
