import { Client } from "minio";
import logger from "../utils/logger";

class StorageService {
  private client: Client;
  private externalClient: Client;
  private bucket: string;
  private externalEndpoint: string;

  constructor() {
    const endpoint = process.env.MINIO_ENDPOINT || "localhost:9000";
    const [host, portStr] = endpoint.split(":");
    const port = parseInt(portStr || "9000", 10);

    // Internal client for server-to-MinIO operations
    this.client = new Client({
      endPoint: host,
      port: port,
      useSSL: process.env.MINIO_USE_SSL === "true",
      accessKey: process.env.MINIO_ACCESS_KEY || "minioadmin",
      secretKey: process.env.MINIO_SECRET_KEY || "minioadmin",
    });

    // External client for generating presigned URLs accessible from host
    this.externalEndpoint =
      process.env.MINIO_EXTERNAL_ENDPOINT || "localhost:9000";
    const [externalHost, externalPortStr] = this.externalEndpoint.split(":");
    const externalPort = parseInt(externalPortStr || "9000", 10);

    this.externalClient = new Client({
      endPoint: externalHost,
      port: externalPort,
      useSSL: process.env.MINIO_USE_SSL === "true",
      accessKey: process.env.MINIO_ACCESS_KEY || "minioadmin",
      secretKey: process.env.MINIO_SECRET_KEY || "minioadmin",
    });

    this.bucket = process.env.MINIO_BUCKET || "silentmode-uploads";
    logger.info(
      `StorageService initialized with endpoint: ${endpoint}, external: ${this.externalEndpoint}, bucket: ${this.bucket}`,
    );
  }

  async ensureBucket(): Promise<void> {
    try {
      const exists = await this.client.bucketExists(this.bucket);
      if (!exists) {
        await this.client.makeBucket(this.bucket, "us-east-1");
        logger.info(`Created bucket: ${this.bucket}`);
      } else {
        logger.info(`Bucket already exists: ${this.bucket}`);
      }
    } catch (error) {
      logger.error("Error ensuring bucket exists:", error);
      throw error;
    }
  }

  async generatePresignedPutUrl(
    objectKey: string,
    expiresIn: number = 900,
  ): Promise<string> {
    try {
      const url = await this.client.presignedPutObject(
        this.bucket,
        objectKey,
        expiresIn,
      );
      logger.debug(
        `Generated presigned PUT URL for ${objectKey}, expires in ${expiresIn}s`,
      );
      // PUT URLs are used by clients inside Docker, so keep internal hostname
      return url;
    } catch (error) {
      logger.error(
        `Error generating presigned PUT URL for ${objectKey}:`,
        error,
      );
      throw error;
    }
  }

  async generatePresignedGetUrl(
    objectKey: string,
    expiresIn: number = 3600,
  ): Promise<string> {
    try {
      // Use external client to generate URL with correct signature for external access
      const url = await this.externalClient.presignedGetObject(
        this.bucket,
        objectKey,
        expiresIn,
      );
      logger.debug(
        `Generated presigned GET URL for ${objectKey}, expires in ${expiresIn}s`,
      );
      return url;
    } catch (error) {
      logger.error(
        `Error generating presigned GET URL for ${objectKey}:`,
        error,
      );
      throw error;
    }
  }

  async verifyObjectExists(objectKey: string): Promise<boolean> {
    try {
      await this.client.statObject(this.bucket, objectKey);
      logger.debug(`Object verified: ${objectKey}`);
      return true;
    } catch (error: any) {
      if (error.code === "NotFound") {
        logger.debug(`Object not found: ${objectKey}`);
        return false;
      }
      logger.error(`Error verifying object ${objectKey}:`, error);
      throw error;
    }
  }

  async getObjectMetadata(
    objectKey: string,
  ): Promise<{ size: number; etag: string } | null> {
    try {
      const stat = await this.client.statObject(this.bucket, objectKey);
      return {
        size: stat.size,
        etag: stat.etag,
      };
    } catch (error: any) {
      if (error.code === "NotFound") {
        return null;
      }
      logger.error(`Error getting object metadata for ${objectKey}:`, error);
      throw error;
    }
  }

  private replaceHostInUrl(url: string): string {
    const internalEndpoint = process.env.MINIO_ENDPOINT || "localhost:9000";
    return url.replace(internalEndpoint, this.externalEndpoint);
  }
}

export default new StorageService();
