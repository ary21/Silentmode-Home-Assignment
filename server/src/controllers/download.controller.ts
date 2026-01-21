import { Request, Response } from "express";
import Joi from "joi";
import downloadService from "../services/download.service";
import logger from "../utils/logger";

// Validation schemas
const triggerDownloadSchema = Joi.object({
  reason: Joi.string().optional(),
  requestedBy: Joi.string().email().optional(),
  originalFilename: Joi.string().max(255).optional(), // New optional field
});

const clientIdSchema = Joi.string()
  .pattern(/^[a-zA-Z0-9_-]+$/)
  .required();
const downloadIdSchema = Joi.string().uuid().required();

export async function triggerDownload(
  req: Request,
  res: Response,
): Promise<void> {
  try {
    const { clientId } = req.params;

    // Validate clientId
    const { error: clientIdError } = clientIdSchema.validate(clientId);
    if (clientIdError) {
      res.status(400).json({
        error: "Invalid clientId format",
        details: clientIdError.message,
      });
      return;
    }

    // Validate request body
    const { error: bodyError, value } = triggerDownloadSchema.validate(
      req.body || {},
    );
    if (bodyError) {
      res
        .status(400)
        .json({ error: "Invalid request body", details: bodyError.message });
      return;
    }

    const meta = value.requestedBy
      ? { requestedBy: value.requestedBy }
      : undefined;

    // Pass originalFilename to service
    const record = await downloadService.triggerDownload(
      clientId,
      value.originalFilename,
      meta,
    );

    res.status(200).json({
      ok: true,
      downloadId: record.downloadId,
      objectKey: record.objectKey,
      expiresAt: record.expiresAt,
    });
  } catch (error) {
    logger.error("Error in triggerDownload controller:", error);
    res.status(500).json({ error: "Internal server error" });
  }
}

export async function getDownloadStatus(
  req: Request,
  res: Response,
): Promise<void> {
  try {
    const { downloadId } = req.params;

    // Validate downloadId
    const { error } = downloadIdSchema.validate(downloadId);
    if (error) {
      res
        .status(400)
        .json({ error: "Invalid downloadId format", details: error.message });
      return;
    }

    // Await the async service call
    const record = await downloadService.getDownloadStatus(downloadId);

    if (!record) {
      res.status(404).json({ error: "Download not found" });
      return;
    }

    // Return record without presignedUrl (security)
    const { presignedUrl, ...publicRecord } = record;
    res.status(200).json(publicRecord);
  } catch (error) {
    logger.error("Error in getDownloadStatus controller:", error);
    res.status(500).json({ error: "Internal server error" });
  }
}

export async function listClients(req: Request, res: Response): Promise<void> {
  try {
    // Current implementation of getClientIds is async stub
    const clientIds = await downloadService.getClientIds();
    res.status(200).json({ clients: clientIds });
  } catch (error) {
    logger.error("Error in listClients controller:", error);
    res.status(500).json({ error: "Internal server error" });
  }
}

export async function getArtifact(req: Request, res: Response): Promise<void> {
  try {
    const { downloadId } = req.params;

    // Validate downloadId
    const { error } = downloadIdSchema.validate(downloadId);
    if (error) {
      res
        .status(400)
        .json({ error: "Invalid downloadId format", details: error.message });
      return;
    }

    const url = await downloadService.getArtifactUrl(downloadId);

    if (!url) {
      res
        .status(404)
        .json({ error: "Artifact not available or download not verified" });
      return;
    }

    res.status(200).json({
      downloadId,
      artifactUrl: url,
      expiresIn: 3600, // 1 hour
    });
  } catch (error) {
    logger.error("Error in getArtifact controller:", error);
    res.status(500).json({ error: "Internal server error" });
  }
}

export async function healthCheck(req: Request, res: Response): Promise<void> {
  res
    .status(200)
    .json({ status: "healthy", timestamp: new Date().toISOString() });
}
