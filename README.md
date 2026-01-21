# SilentMode - Command Queue + Presigned URL File Download System

A distributed system for orchestrating file downloads from on-premise clients behind NAT using command queues and presigned URLs. The system enables cloud servers to trigger file uploads from clients that cannot accept inbound connections, using Redis for messaging and MinIO/S3 for object storage.

## Architecture Overview

```mermaid
graph TB
    subgraph Cloud
        Server[Server API]
        Storage[MinIO/S3]
        Broker[Redis Broker]
    end

    subgraph OnPremise["On-Premise (NAT)"]
        Client1[Client Agent 1]
        Client2[Client Agent 2]
    end

    Operator[Operator] -->|1. POST /download/:clientId| Server
    Server -->|2. Generate presigned PUT URL| Storage
    Server -->|3. Publish command| Broker
    Client1 & Client2 -->|4. Subscribe to commands| Broker
    Client1 & Client2 -->|5. Upload file| Storage
    Client1 & Client2 -->|6. Publish event| Broker
    Broker -->|7. Notify upload complete| Server
    Server -->|8. Verify object| Storage
```

## Key Features

- **NAT-Friendly**: Clients only make outbound connections
- **Direct Upload**: Files stream directly from client to object storage (no server bottleneck)
- **Resilient**: Automatic retry with exponential backoff
- **Checksum Verification**: SHA256 validation ensures data integrity
- **Multi-Client**: Supports multiple simultaneous clients
- **Presigned URLs**: Temporary, secure access to object storage
- **Real-time Status**: Track download progress via API

## System Flow

1. **Trigger Download**: Operator calls `POST /download/:clientId`
2. **Generate Presigned URL**: Server creates temporary PUT URL for S3/MinIO
3. **Send Command**: Server publishes upload command to Redis channel `commands:<clientId>`
4. **Client Receives**: Client agent receives command via Redis subscription
5. **Upload File**: Client streams file to presigned URL with checksum calculation
6. **Publish Event**: Client publishes `upload_complete` event with metadata
7. **Verify Upload**: Server validates object exists and checksum matches
8. **Mark Complete**: Download status updated to `verified`

## Prerequisites

- Docker & Docker Compose
- 100MB disk space for test files
- Ports available: 8080 (server), 9000 (MinIO API), 9001 (MinIO Console), 6379 (Redis)

## Quick Start

### 1. Clone and Setup

```bash
git clone git@github.com:ary21/Silentmode-Home-Assignment.git
cd Silentmode-Home-Assignment
cp .env.example .env
```

Edit `.env` and set your `SERVER_API_KEY`:

```env
SERVER_API_KEY=your-secret-api-key-here
```

### 2. Start Services

```bash
docker compose up --build
```

This starts:

- **MinIO** (S3-compatible storage) - Console at http://localhost:9001
- **Redis** (Message broker)
- **Server** (API) - http://localhost:8080
- **Client 1 & 2** (On-premise agents)

### 3. Create Bucket (First Time Only)

Access MinIO Console at http://localhost:9001

- Username: `minioadmin`
- Password: `minioadmin`
- Create bucket named: `uploads`

### 4. Create Test File

```bash
# In client-1 container
docker exec silentmode-client-1 sh -c "dd if=/dev/urandom of=/root/file_to_download.txt bs=1M count=100"

# In client-2 container
docker exec silentmode-client-2 sh -c "dd if=/dev/urandom of=/root/file_to_download.txt bs=1M count=100"
```

### 5. Trigger Download

```bash
curl -X POST http://localhost:8080/download/client-1 \
  -H "Authorization: Bearer your-secret-api-key-here" \
  -H "Content-Type: application/json" \
  -d '{"reason":"manual test"}'
```

Response:

```json
{
  "ok": true,
  "downloadId": "550e8400-e29b-41d4-a716-446655440000",
  "objectKey": "uploads/client-1/550e8400-e29b-41d4-a716-446655440000.bin",
  "expiresAt": "2026-01-21T08:00:00.000Z"
}
```

### 6. Check Status

```bash
curl http://localhost:8080/downloads/<downloadId> \
  -H "Authorization: Bearer your-secret-api-key-here"
```

Response:

```json
{
  "downloadId": "550e8400-e29b-41d4-a716-446655440000",
  "clientId": "client-1",
  "status": "verified",
  "objectKey": "uploads/client-1/550e8400-e29b-41d4-a716-446655440000.bin",
  "size": 104857600,
  "sha256": "abc123...",
  "createdAt": "2026-01-21T07:45:00.000Z",
  "updatedAt": "2026-01-21T07:46:30.000Z"
}
```

Status values: `pending` → `uploaded` → `verified` (or `failed`)

### 7. Download Artifact

```bash
curl http://localhost:8080/download/<downloadId>/artifacts \
  -H "Authorization: Bearer your-secret-api-key-here"
```

Response:

```json
{
  "downloadId": "550e8400-e29b-41d4-a716-446655440000",
  "artifactUrl": "http://minio:9000/uploads/...",
  "expiresIn": 3600
}
```

Use the `artifactUrl` to download the file:

```bash
curl -o myfile.bin "<artifactUrl>"
```

## API Reference

### Authentication

All endpoints (except `/health`) require Bearer token authentication:

```
Authorization: Bearer <SERVER_API_KEY>
```

### Endpoints

#### `GET /health`

Health check endpoint (no authentication required).

**Response:**

```json
{
  "status": "healthy",
  "timestamp": "2026-01-21T07:00:00.000Z"
}
```

#### `POST /download/:clientId`

Trigger file download from specified client.

**Parameters:**

- `clientId` (path): Client identifier (alphanumeric, hyphens, underscores)

**Request Body (optional):**

```json
{
  "reason": "manual test",
  "requestedBy": "operator@example.com"
}
```

**Response:**

```json
{
  "ok": true,
  "downloadId": "uuid",
  "objectKey": "uploads/client-1/uuid.bin",
  "expiresAt": "ISO8601 timestamp"
}
```

#### `GET /downloads/:downloadId`

Get download status and details.

**Response:**

```json
{
  "downloadId": "uuid",
  "clientId": "client-1",
  "status": "pending|uploaded|verified|failed",
  "objectKey": "uploads/client-1/uuid.bin",
  "size": 104857600,
  "sha256": "hex checksum",
  "createdAt": "ISO8601",
  "updatedAt": "ISO8601",
  "error": "error message (if failed)"
}
```

#### `GET /clients`

List all known client IDs.

**Response:**

```json
{
  "clients": ["client-1", "client-2"]
}
```

#### `GET /download/:downloadId/artifacts`

Get presigned download URL for verified artifact.

**Response:**

```json
{
  "downloadId": "uuid",
  "artifactUrl": "presigned S3 GET URL",
  "expiresIn": 3600
}
```

## Message Schemas

### Command (Server → Client)

Published to: `commands:<clientId>`

```json
{
  "cmd": "upload",
  "downloadId": "uuid",
  "objectKey": "uploads/client-1/uuid.bin",
  "presignedUrl": "https://minio:9000/...",
  "expiresAt": "ISO8601 timestamp",
  "meta": {
    "requestedBy": "operator@example.com"
  }
}
```

### Event (Client → Server)

Published to: `events:server`

**Upload Complete:**

```json
{
  "event": "upload_complete",
  "downloadId": "uuid",
  "objectKey": "uploads/client-1/uuid.bin",
  "size": 104857600,
  "sha256": "hex checksum",
  "status": "ok",
  "timestamp": "ISO8601"
}
```

**Upload Failed:**

```json
{
  "event": "upload_failed",
  "downloadId": "uuid",
  "objectKey": "uploads/client-1/uuid.bin",
  "reason": "error message",
  "timestamp": "ISO8601"
}
```

## Automated Testing

### E2E Test Script

Run the full end-to-end test:

```bash
./scripts/e2e-test.sh
```

This script:

1. Checks server health
2. Creates 100MB test file in client container
3. Triggers download
4. Polls status until verified
5. Validates artifact accessibility
6. Reports results with colored output

Expected output:

```
=== SilentMode E2E Test ===

→ Checking server health...
✓ Server is healthy
→ Creating test file in client container...
✓ Test file created (100MB)
→ Triggering download for client-1...
✓ Download triggered: 550e8400-e29b-41d4-a716-446655440000
→ Waiting for upload to complete...
..........
✓ Upload completed and verified
✓ Download details:
  - Download ID: 550e8400-e29b-41d4-a716-446655440000
  - Status: verified
  - Size: 100MiB
  - SHA256: abc123...
✓ Artifact is accessible

=== E2E Test Passed ===
```

### Manual Test File Creation

```bash
# On host machine
./scripts/create-test-file.sh [size_in_mb]

# Inside container
docker exec <container> /app/scripts/create-test-file.sh
```

## Configuration

### Environment Variables

**Server:**

- `MINIO_ENDPOINT` - MinIO host:port (default: `minio:9000`)
- `MINIO_ACCESS_KEY` - MinIO access key (default: `minioadmin`)
- `MINIO_SECRET_KEY` - MinIO secret key (default: `minioadmin`)
- `MINIO_BUCKET` - Bucket name (default: `uploads`)
- `REDIS_URL` - Redis connection string (default: `redis://redis:6379`)
- `SERVER_PORT` - HTTP port (default: `8080`)
- `SERVER_API_KEY` - API authentication key (required)
- `PRESIGNED_EXPIRES_SEC` - Presigned URL expiry (default: `900`)

**Client:**

- `CLIENT_ID` - Unique client identifier (required)
- `REDIS_URL` - Redis connection string (default: `redis://redis:6379`)
- `FILE_PATH` - Path to file to upload (default: `/root/file_to_download.txt`)

## Troubleshooting

### Client not receiving commands

**Check Redis connection:**

```bash
docker logs silentmode-client-1
```

Look for: `Subscribed to commands:client-1`

**Test Redis manually:**

```bash
docker exec -it silentmode-redis redis-cli
> PUBLISH commands:client-1 '{"test":"message"}'
```

### Upload fails with presigned URL expired

**Solution:** Increase `PRESIGNED_EXPIRES_SEC` or ensure client processes commands faster.

Default is 15 minutes (900 seconds). For slower networks:

```env
PRESIGNED_EXPIRES_SEC=1800  # 30 minutes
```

### Server can't verify upload

**Check MinIO bucket exists:**

```bash
docker exec -it silentmode-minio mc ls minio/uploads
```

**View server logs:**

```bash
docker logs silentmode-server
```

### File not found in client

**Verify file path:**

```bash
docker exec silentmode-client-1 ls -lh /root/file_to_download.txt
```

**Create test file:**

```bash
docker exec silentmode-client-1 dd if=/dev/urandom of=/root/file_to_download.txt bs=1M count=100
```

## Architecture Details

### Component Responsibilities

**Server:**

- Generate presigned URLs
- Publish commands to Redis
- Listen for upload events
- Verify uploads (size, checksum)
- Provide download status API
- Issue presigned download URLs

**Client:**

- Subscribe to command channel
- Validate commands (auth, expiry)
- Stream file upload to presigned URL
- Calculate SHA256 during upload
- Publish completion events
- Retry failed uploads (exponential backoff)
- Prevent duplicate processing

**MinIO:**

- Store uploaded files
- Validate presigned URL signatures
- Serve authenticated downloads

**Redis:**

- Route commands to specific clients
- Broadcast events to server
- Lightweight pub/sub (no persistence needed)

### Reliability Features

1. **Retry Logic**: Client retries failed uploads up to 5 times with exponential backoff (1s, 2s, 4s, 8s, 16s)
2. **Duplicate Prevention**: Client rejects commands for downloads already in progress
3. **Expiry Validation**: Client checks presigned URL expiry before upload
4. **Checksum Verification**: Server validates SHA256 against reported value
5. **Size Validation**: Server confirms object size matches reported size
6. **Graceful Shutdown**: Both server and client handle SIGTERM/SIGINT

### Security Considerations

**Production Recommendations:**

1. **Use HTTPS**: Enable SSL for MinIO (`MINIO_USE_SSL=true`)
2. **Strong API Keys**: Use cryptographically random keys (not `dev-api-key-change-in-production`)
3. **Command Signing**: Add HMAC signatures to commands (prevents command injection)
4. **Network Isolation**: Use private networks for Redis and MinIO
5. **Presigned URL Expiry**: Keep short (15 min default) to minimize exposure window
6. **Access Control**: Limit MinIO bucket access to server service account

## Development

### Project Structure

```
silentmode/
├── server/               # Cloud server application
│   ├── src/
│   │   ├── controllers/ # API endpoint handlers
│   │   ├── services/    # Business logic (storage, broker, download)
│   │   ├── middleware/  # Authentication
│   │   ├── models/      # TypeScript interfaces
│   │   └── utils/       # Logger
│   ├── Dockerfile
│   ├── package.json
│   └── tsconfig.json
├── client/              # On-premise client agent
│   ├── src/
│   │   ├── services/    # Broker, uploader
│   │   └── utils/       # Logger, checksum
│   ├── Dockerfile
│   ├── package.json
│   └── tsconfig.json
├── scripts/             # Test utilities
│   ├── create-test-file.sh
│   └── e2e-test.sh
├── docker-compose.yml
├── .env.example
└── README.md
```

### Local Development

**Server:**

```bash
cd server
npm install
npm run dev  # ts-node with watch mode
```

**Client:**

```bash
cd client
npm install
npm run dev
```

**Environment:**
Ensure Redis and MinIO are running:

```bash
docker compose up redis minio
```

### Building

```bash
# Build server
cd server && npm run build

# Build client
cd client && npm run build
```

Compiled output in `dist/` directory.

## Performance

### Throughput

- **Upload**: Limited by client network bandwidth (direct to MinIO)
- **Concurrent Downloads**: Unlimited (no server bottleneck)
- **API**: Express.js handles thousands of requests/sec

### Scalability

- **Horizontal Server**: Scale server instances (stateless, uses in-memory storage)
- **Horizontal Clients**: Add clients as needed (each has unique CLIENT_ID)
- **Storage**: MinIO supports distributed mode for petabytes
- **Broker**: Redis Cluster for high availability

### Resource Usage

- **Server**: ~50MB RAM, <1% CPU idle
- **Client**: ~30MB RAM, <1% CPU idle, spikes during upload
- **MinIO**: ~100MB RAM + storage
- **Redis**: ~10MB RAM

## License

MIT

## Support

For issues or questions, check:

1. Logs: `docker compose logs -f <service>`
2. Health: `curl http://localhost:8080/health`
3. MinIO Console: http://localhost:9001
4. Redis: `docker exec -it silentmode-redis redis-cli MONITOR`
