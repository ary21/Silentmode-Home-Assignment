import { v4 as uuidv4 } from "uuid";
import storageService from "./storage.service";
import brokerService from "./broker.service";
import logger from "../utils/logger";
import {
  DownloadRecord,
  UploadCommand,
  UploadEvent,
} from "../models/download.model";

class DownloadService {
  private downloads: Map<string, DownloadRecord> = new Map();
  private presignedExpiresSeconds: number;

  constructor() {
    this.presignedExpiresSeconds = parseInt(
      process.env.PRESIGNED_EXPIRES_SEC || "900",
      10,
    );
  }

  async triggerDownload(
    clientId: string,
    meta?: { requestedBy?: string },
  ): Promise<DownloadRecord> {
    try {
      const downloadId = uuidv4();
      const objectKey = `silentmode-uploads/${clientId}/${downloadId}.bin`;
      const now = new Date().toISOString();
      const expiresAt = new Date(
        Date.now() + this.presignedExpiresSeconds * 1000,
      ).toISOString();

      // Generate presigned PUT URL
      const presignedUrl = await storageService.generatePresignedPutUrl(
        objectKey,
        this.presignedExpiresSeconds,
      );

      // Create download record
      const record: DownloadRecord = {
        downloadId,
        clientId,
        objectKey,
        status: "pending",
        presignedUrl,
        expiresAt,
        createdAt: now,
        updatedAt: now,
        meta,
      };

      this.downloads.set(downloadId, record);

      // Publish command to client
      const command: UploadCommand = {
        cmd: "upload",
        downloadId,
        objectKey,
        presignedUrl,
        expiresAt,
        meta,
      };

      await brokerService.publishCommand(clientId, command);

      logger.info(`Triggered download for client ${clientId}`, {
        downloadId,
        objectKey,
      });

      return record;
    } catch (error) {
      logger.error(`Error triggering download for client ${clientId}:`, error);
      throw error;
    }
  }

  async handleUploadComplete(event: UploadEvent): Promise<void> {
    const { downloadId } = event;
    const record = this.downloads.get(downloadId);

    if (!record) {
      logger.warn(`Received event for unknown downloadId: ${downloadId}`);
      return;
    }

    try {
      if (event.event === "upload_complete") {
        // Update record with upload info
        record.status = "uploaded";
        record.size = event.size;
        record.sha256 = event.sha256;
        record.updatedAt = new Date().toISOString();

        logger.info(`Upload complete for ${downloadId}`, {
          size: event.size,
          sha256: event.sha256,
        });

        // Verify object exists in storage
        const exists = await storageService.verifyObjectExists(
          record.objectKey,
        );

        if (!exists) {
          record.status = "failed";
          record.error = "Object not found in storage after upload";
          logger.error(
            `Verification failed: object not found for ${downloadId}`,
          );
          return;
        }

        // Get metadata and verify size
        const metadata = await storageService.getObjectMetadata(
          record.objectKey,
        );

        if (!metadata) {
          record.status = "failed";
          record.error = "Could not retrieve object metadata";
          logger.error(
            `Verification failed: could not get metadata for ${downloadId}`,
          );
          return;
        }

        if (metadata.size !== event.size) {
          record.status = "failed";
          record.error = `Size mismatch: expected ${event.size}, got ${metadata.size}`;
          logger.error(`Verification failed: size mismatch for ${downloadId}`, {
            expected: event.size,
            actual: metadata.size,
          });
          return;
        }

        // Mark as verified
        record.status = "verified";
        record.updatedAt = new Date().toISOString();
        logger.info(`Download verified successfully: ${downloadId}`);
      } else if (event.event === "upload_failed") {
        record.status = "failed";
        record.error = event.reason;
        record.updatedAt = new Date().toISOString();
        logger.error(`Upload failed for ${downloadId}: ${event.reason}`);
      }
    } catch (error) {
      logger.error(`Error handling upload complete for ${downloadId}:`, error);
      record.status = "failed";
      record.error = error instanceof Error ? error.message : "Unknown error";
      record.updatedAt = new Date().toISOString();
    }
  }

  getDownloadStatus(downloadId: string): DownloadRecord | null {
    return this.downloads.get(downloadId) || null;
  }

  getAllDownloads(): DownloadRecord[] {
    return Array.from(this.downloads.values());
  }

  getClientIds(): string[] {
    const clientIds = new Set<string>();
    this.downloads.forEach((record) => clientIds.add(record.clientId));
    return Array.from(clientIds);
  }

  async getArtifactUrl(downloadId: string): Promise<string | null> {
    const record = this.downloads.get(downloadId);

    if (!record || record.status !== "verified") {
      return null;
    }

    try {
      const url = await storageService.generatePresignedGetUrl(
        record.objectKey,
      );
      return url;
    } catch (error) {
      logger.error(`Error generating artifact URL for ${downloadId}:`, error);
      throw error;
    }
  }
}

export default new DownloadService();
