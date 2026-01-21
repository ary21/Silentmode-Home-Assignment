import { createClient, RedisClientType } from "redis";
import logger from "../utils/logger";
import { UploadCommand, UploadEvent } from "../models/download.model";

class BrokerService {
  private publisher: RedisClientType;
  private subscriber: RedisClientType;
  private isConnected: boolean = false;

  constructor() {
    const redisUrl = process.env.REDIS_URL || "redis://localhost:6379";

    this.publisher = createClient({ url: redisUrl });
    this.subscriber = createClient({ url: redisUrl });

    this.publisher.on("error", (err) =>
      logger.error("Redis Publisher Error:", err),
    );
    this.subscriber.on("error", (err) =>
      logger.error("Redis Subscriber Error:", err),
    );
  }

  async connect(): Promise<void> {
    try {
      await this.publisher.connect();
      await this.subscriber.connect();
      this.isConnected = true;
      logger.info("BrokerService connected to Redis");
    } catch (error) {
      logger.error("Error connecting to Redis:", error);
      throw error;
    }
  }

  async disconnect(): Promise<void> {
    try {
      await this.publisher.quit();
      await this.subscriber.quit();
      this.isConnected = false;
      logger.info("BrokerService disconnected from Redis");
    } catch (error) {
      logger.error("Error disconnecting from Redis:", error);
    }
  }

  async publishCommand(
    clientId: string,
    command: UploadCommand,
  ): Promise<void> {
    if (!this.isConnected) {
      throw new Error("BrokerService not connected");
    }

    try {
      const channel = `commands:${clientId}`;
      const message = JSON.stringify(command);
      await this.publisher.publish(channel, message);
      logger.info(`Published command to ${channel}`, {
        downloadId: command.downloadId,
      });
    } catch (error) {
      logger.error(`Error publishing command to client ${clientId}:`, error);
      throw error;
    }
  }

  async subscribeToEvents(
    callback: (event: UploadEvent) => void,
  ): Promise<void> {
    if (!this.isConnected) {
      throw new Error("BrokerService not connected");
    }

    try {
      const channel = "events:server";

      await this.subscriber.subscribe(channel, (message) => {
        try {
          const event = JSON.parse(message) as UploadEvent;
          logger.debug(`Received event from ${channel}`, {
            event: event.event,
          });
          callback(event);
        } catch (error) {
          logger.error("Error parsing event message:", error);
        }
      });

      logger.info(`Subscribed to ${channel}`);
    } catch (error) {
      logger.error("Error subscribing to events:", error);
      throw error;
    }
  }
}

export default new BrokerService();
