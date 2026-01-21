export interface DownloadRecord {
  downloadId: string;
  clientId: string;
  objectKey: string;
  status: "pending" | "uploaded" | "verified" | "failed";
  presignedUrl?: string;
  expiresAt?: string;
  size?: number;
  sha256?: string;
  createdAt: string;
  updatedAt: string;
  error?: string;
  meta?: {
    requestedBy?: string;
  };
}

export interface UploadCommand {
  cmd: "upload";
  downloadId: string;
  objectKey: string;
  presignedUrl: string;
  expiresAt: string;
  meta?: {
    requestedBy?: string;
  };
}

export interface UploadCompleteEvent {
  event: "upload_complete";
  downloadId: string;
  objectKey: string;
  size: number;
  sha256: string;
  status: "ok";
  timestamp: string;
}

export interface UploadFailedEvent {
  event: "upload_failed";
  downloadId: string;
  objectKey: string;
  reason: string;
  timestamp: string;
}

export type UploadEvent = UploadCompleteEvent | UploadFailedEvent;
