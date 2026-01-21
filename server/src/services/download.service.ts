import { v4 as uuidv4 } from "uuid";
import storageService from "./storage.service";
import brokerService from "./broker.service";
import dbService from "./db.service";
import logger from "../utils/logger";
import { sanitizeFilename } from "../utils/sanitizer";
import {
  DownloadRecord,
  UploadCommand,
  UploadEvent,
} from "../models/download.model";

class DownloadService {
  private presignedExpiresSeconds: number;

  constructor() {
    this.presignedExpiresSeconds = parseInt(
      process.env.PRESIGNED_EXPIRES_SEC || "900",
      10,
    );
  }

  async triggerDownload(
    clientId: string,
    originalFilename?: string,
    meta?: { requestedBy?: string },
  ): Promise<DownloadRecord> {
    try {
      const downloadId = uuidv4();
      const sanitizedName = sanitizeFilename(originalFilename);
      // New object key format: clientId/downloadId-sanitizedFilename
      const objectKey = `${clientId}/${downloadId}-${sanitizedName}`;

      const now = new Date().toISOString();
      const expiresAt = new Date(
        Date.now() + this.presignedExpiresSeconds * 1000,
      ).toISOString();

      // Generate presigned PUT URL
      const presignedUrl = await storageService.generatePresignedPutUrl(
        objectKey,
        this.presignedExpiresSeconds,
      );

      // Create download record in DB
      await dbService.createDownload({
        download_id: downloadId,
        client_id: clientId,
        object_key: objectKey,
        original_filename: originalFilename || sanitizedName,
        status: "pending",
        presigned_expires_at: expiresAt,
      });

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
        originalFilename,
      });

      return record;
    } catch (error) {
      logger.error(`Error triggering download for client ${clientId}:`, error);
      throw error;
    }
  }

  async handleUploadComplete(event: UploadEvent): Promise<void> {
    const { downloadId } = event;

    try {
      const record = await dbService.getDownload(downloadId);
      if (!record) {
        logger.warn(`Received event for unknown downloadId: ${downloadId}`);
        return;
      }

      if (event.event === "upload_complete") {
        await dbService.updateDownloadStatus(downloadId, "uploaded", {
          size: event.size,
          sha256: event.sha256,
          content_type: "application/octet-stream",
        });

        logger.info(`Upload complete for ${downloadId}`, {
          size: event.size,
          sha256: event.sha256,
        });

        const exists = await storageService.verifyObjectExists(
          record.object_key,
        );

        if (!exists) {
          await dbService.updateDownloadStatus(downloadId, "failed");
          logger.error(
            `Verification failed: object not found for ${downloadId}`,
          );
          return;
        }

        const metadata = await storageService.getObjectMetadata(
          record.object_key,
        );

        if (!metadata) {
          await dbService.updateDownloadStatus(downloadId, "failed");
          logger.error(
            `Verification failed: could not get metadata for ${downloadId}`,
          );
          return;
        }

        if (metadata.size !== event.size) {
          await dbService.updateDownloadStatus(downloadId, "failed");
          logger.error(`Verification failed: size mismatch for ${downloadId}`, {
            expected: event.size,
            actual: metadata.size,
          });
          return;
        }

        await dbService.updateDownloadStatus(downloadId, "verified");
        logger.info(`Download verified successfully: ${downloadId}`);
      } else if (event.event === "upload_failed") {
        await dbService.updateDownloadStatus(downloadId, "failed");
        logger.error(`Upload failed for ${downloadId}: ${event.reason}`);
      }
    } catch (error) {
      logger.error(`Error handling upload complete for ${downloadId}:`, error);
      try {
        await dbService.updateDownloadStatus(downloadId, "failed");
      } catch (e) {
        /* ignore */
      }
    }
  }

  async getDownloadStatus(downloadId: string): Promise<DownloadRecord | null> {
    const record = await dbService.getDownload(downloadId);
    if (!record) return null;

    return {
      downloadId: record.download_id,
      clientId: record.client_id,
      objectKey: record.object_key,
      status: record.status as any,
      createdAt: record.created_at || "",
      updatedAt: record.updated_at || "",
      size: record.size,
      sha256: record.sha256,
    };
  }

  async getAllDownloads(clientId: string): Promise<DownloadRecord[]> {
    const records = await dbService.listDownloads(clientId);
    return records.map((r) => ({
      downloadId: r.download_id,
      clientId: r.client_id,
      objectKey: r.object_key,
      status: r.status as any,
      createdAt: r.created_at || "",
      updatedAt: r.updated_at || "",
      size: r.size,
      sha256: r.sha256,
    }));
  }

  async getClientIds(): Promise<string[]> {
    return [];
  }

  async getArtifactUrl(downloadId: string): Promise<string | null> {
    const record = await dbService.getDownload(downloadId);

    if (!record || record.status !== "verified") {
      return null;
    }

    try {
      const url = await storageService.generatePresignedGetUrl(
        record.object_key,
        3600,
        record.original_filename,
      );
      return url;
    } catch (error) {
      logger.error(`Error generating artifact URL for ${downloadId}:`, error);
      throw error;
    }
  }
}

export default new DownloadService();
