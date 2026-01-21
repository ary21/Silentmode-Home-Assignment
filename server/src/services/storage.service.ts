import { Client } from "minio";
import logger from "../utils/logger";

class StorageService {
  private client: Client;
  private bucket: string;

  constructor() {
    const endpoint = process.env.MINIO_ENDPOINT || "localhost:9000";
    const [host, portStr] = endpoint.split(":");
    const port = parseInt(portStr || "9000", 10);

    this.client = new Client({
      endPoint: host,
      port: port,
      useSSL: process.env.MINIO_USE_SSL === "true",
      accessKey: process.env.MINIO_ACCESS_KEY || "minioadmin",
      secretKey: process.env.MINIO_SECRET_KEY || "minioadmin",
    });

    this.bucket = process.env.MINIO_BUCKET || "silentmode-uploads";
    logger.info(
      `StorageService initialized with endpoint: ${endpoint}, bucket: ${this.bucket}`,
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
      const url = await this.client.presignedGetObject(
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
}

export default new StorageService();
