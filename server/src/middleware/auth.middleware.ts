import { Request, Response, NextFunction } from "express";
import logger from "../utils/logger";

export function authMiddleware(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  const authHeader = req.headers.authorization;

  if (!authHeader || !authHeader.startsWith("Bearer ")) {
    logger.warn(
      "Authentication failed: missing or invalid authorization header",
    );
    res
      .status(401)
      .json({ error: "Unauthorized: missing or invalid authorization header" });
    return;
  }

  const token = authHeader.substring(7); // Remove 'Bearer ' prefix
  const expectedToken = process.env.SERVER_API_KEY;

  if (!expectedToken) {
    logger.error("SERVER_API_KEY not configured");
    res.status(500).json({ error: "Server configuration error" });
    return;
  }

  if (token !== expectedToken) {
    logger.warn("Authentication failed: invalid API key");
    res.status(401).json({ error: "Unauthorized: invalid API key" });
    return;
  }

  next();
}
