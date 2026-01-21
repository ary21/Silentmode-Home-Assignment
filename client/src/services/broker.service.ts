import { createClient, RedisClientType } from "redis";
import logger from "../utils/logger";

interface UploadCommand {
  cmd: "upload";
  downloadId: string;
  objectKey: string;
  presignedUrl: string;
  expiresAt: string;
  meta?: any;
}

interface UploadEvent {
  event: "upload_complete" | "upload_failed";
  downloadId: string;
  objectKey: string;
  size?: number;
  sha256?: string;
  status?: "ok";
  reason?: string;
  timestamp: string;
}

class BrokerService {
  private subscriber: RedisClientType;
  private publisher: RedisClientType;
  private isConnected: boolean = false;

  constructor() {
    const redisUrl = process.env.REDIS_URL || "redis://localhost:6379";

    this.subscriber = createClient({ url: redisUrl });
    this.publisher = createClient({ url: redisUrl });

    this.subscriber.on("error", (err) =>
      logger.error("Redis Subscriber Error:", err),
    );
    this.publisher.on("error", (err) =>
      logger.error("Redis Publisher Error:", err),
    );
  }

  async connect(): Promise<void> {
    try {
      await this.subscriber.connect();
      await this.publisher.connect();
      this.isConnected = true;
      logger.info("BrokerService connected to Redis");
    } catch (error) {
      logger.error("Error connecting to Redis:", error);
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    try {
      await this.subscriber.quit();
      await this.publisher.quit();
      this.isConnected = false;
      logger.info("BrokerService disconnected from Redis");
    } catch (error) {
      logger.error("Error disconnecting from Redis:", error);
    }
  }

  async subscribeToCommands(
    clientId: string,
    callback: (command: UploadCommand) => void,
  ): Promise<void> {
    if (!this.isConnected) {
      throw new Error("BrokerService not connected");
    }

    try {
      const channel = `commands:${clientId}`;

      await this.subscriber.subscribe(channel, (message) => {
        try {
          const command = JSON.parse(message) as UploadCommand;
          logger.debug(`Received command from ${channel}`, {
            cmd: command.cmd,
          });
          callback(command);
        } catch (error) {
          logger.error("Error parsing command message:", error);
        }
      });

      logger.info(`Subscribed to ${channel}`);
    } catch (error) {
      logger.error("Error subscribing to commands:", error);
      throw error;
    }
  }

  async publishEvent(event: UploadEvent): Promise<void> {
    if (!this.isConnected) {
      throw new Error("BrokerService not connected");
    }

    try {
      const channel = "events:server";
      const message = JSON.stringify(event);
      await this.publisher.publish(channel, message);
      logger.info(`Published event to ${channel}`, { event: event.event });
    } catch (error) {
      logger.error("Error publishing event:", error);
      throw error;
    }
  }
}

export default new BrokerService();
