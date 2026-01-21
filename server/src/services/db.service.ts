import sqlite3 from "sqlite3";
import logger from "../utils/logger";
import path from "path";

export interface Download {
  download_id: string;
  client_id: string;
  object_key: string;
  original_filename: string;
  content_type?: string;
  size?: number;
  sha256?: string;
  status: "pending" | "uploaded" | "verified" | "failed";
  presigned_expires_at?: string;
  created_at?: string;
  updated_at?: string;
}

class DBService {
  private db: sqlite3.Database;

  constructor() {
    const dbPath =
      process.env.DB_PATH || path.join(process.cwd(), "downloads.db");
    this.db = new (sqlite3.verbose().Database)(dbPath, (err) => {
      if (err) {
        logger.error("Could not connect to database", err);
      } else {
        logger.info(`Connected to database at ${dbPath}`);
        this.init();
      }
    });
  }

  private init() {
    this.db.serialize(() => {
      this.db.run(
        `
        CREATE TABLE IF NOT EXISTS downloads (
          download_id TEXT PRIMARY KEY,
          client_id TEXT NOT NULL,
          object_key TEXT NOT NULL,
          original_filename TEXT NOT NULL,
          content_type TEXT,
          size BIGINT,
          sha256 TEXT,
          status TEXT NOT NULL,
          presigned_expires_at TEXT,
          created_at TEXT DEFAULT CURRENT_TIMESTAMP,
          updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
      `,
        (err) => {
          if (err) logger.error("Error creating downloads table", err);
        },
      );

      this.db.run(
        `
        CREATE INDEX IF NOT EXISTS idx_downloads_client ON downloads(client_id)
      `,
        (err) => {
          if (err) logger.error("Error creating index", err);
        },
      );
    });
  }

  async createDownload(
    download: Omit<Download, "created_at" | "updated_at">,
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const sql = `
        INSERT INTO downloads (
          download_id, client_id, object_key, original_filename, 
          status, presigned_expires_at
        ) VALUES (?, ?, ?, ?, ?, ?)
      `;
      this.db.run(
        sql,
        [
          download.download_id,
          download.client_id,
          download.object_key,
          download.original_filename,
          download.status,
          download.presigned_expires_at,
        ],
        (err) => {
          if (err) reject(err);
          else resolve();
        },
      );
    });
  }

  async updateDownloadStatus(
    downloadId: string,
    status: string,
    meta?: Partial<Download>,
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      let sql =
        "UPDATE downloads SET status = ?, updated_at = CURRENT_TIMESTAMP";
      const params: any[] = [status];

      if (meta) {
        if (meta.size !== undefined) {
          sql += ", size = ?";
          params.push(meta.size);
        }
        if (meta.sha256 !== undefined) {
          sql += ", sha256 = ?";
          params.push(meta.sha256);
        }
        if (meta.content_type !== undefined) {
          sql += ", content_type = ?";
          params.push(meta.content_type);
        }
      }

      sql += " WHERE download_id = ?";
      params.push(downloadId);

      this.db.run(sql, params, (err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  async getDownload(downloadId: string): Promise<Download | null> {
    return new Promise((resolve, reject) => {
      this.db.get(
        "SELECT * FROM downloads WHERE download_id = ?",
        [downloadId],
        (err, row) => {
          if (err) reject(err);
          else resolve((row as Download) || null);
        },
      );
    });
  }

  async listDownloads(clientId: string): Promise<Download[]> {
    return new Promise((resolve, reject) => {
      this.db.all(
        "SELECT * FROM downloads WHERE client_id = ? ORDER BY created_at DESC",
        [clientId],
        (err, rows) => {
          if (err) reject(err);
          else resolve((rows as Download[]) || []);
        },
      );
    });
  }
}

export default new DBService();
