#!/bin/bash
set -e

echo "=== SilentMode E2E Test ==="
echo ""

API_KEY="${SERVER_API_KEY:-dev-api-key-change-in-production}"
SERVER_URL="http://localhost:8080"
CLIENT_ID="client-1"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function for colored output
success() {
  echo -e "${GREEN}✓ $1${NC}"
}

error() {
  echo -e "${RED}✗ $1${NC}"
  exit 1
}

info() {
  echo -e "${YELLOW}→ $1${NC}"
}

# Check if server is healthy
info "Checking server health..."
HEALTH=$(curl -s "$SERVER_URL/health")
if [ $? -ne 0 ]; then
  error "Server is not responding"
fi
success "Server is healthy"

# Create test file in client container
info "Creating test file in client container..."
docker exec silentmode-client-1 sh -c "dd if=/dev/urandom of=/root/file_to_download.txt bs=1M count=100 2>/dev/null" > /dev/null
if [ $? -ne 0 ]; then
  error "Failed to create test file"
fi
success "Test file created (100MB)"

# Trigger download
info "Triggering download for $CLIENT_ID..."
RESPONSE=$(curl -s -X POST "$SERVER_URL/download/$CLIENT_ID" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"reason":"e2e-test"}')

if [ $? -ne 0 ]; then
  error "Failed to trigger download"
fi

# Extract downloadId
DOWNLOAD_ID=$(echo $RESPONSE | grep -o '"downloadId":"[^"]*"' | cut -d'"' -f4)
if [ -z "$DOWNLOAD_ID" ]; then
  error "Invalid response: $RESPONSE"
fi

success "Download triggered: $DOWNLOAD_ID"

# Poll for status
info "Waiting for upload to complete..."
MAX_RETRIES=60
RETRY_COUNT=0
STATUS=""

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  STATUS_RESPONSE=$(curl -s "$SERVER_URL/downloads/$DOWNLOAD_ID" \
    -H "Authorization: Bearer $API_KEY")
  
  STATUS=$(echo $STATUS_RESPONSE | grep -o '"status":"[^"]*"' | cut -d'"' -f4)
  
  echo -n "."
  
  if [ "$STATUS" = "verified" ]; then
    echo ""
    success "Upload completed and verified"
    break
  elif [ "$STATUS" = "failed" ]; then
    echo ""
    error "Upload failed: $STATUS_RESPONSE"
  fi
  
  sleep 2
  RETRY_COUNT=$((RETRY_COUNT + 1))
done

if [ "$STATUS" != "verified" ]; then
  error "Upload did not complete within timeout (status: $STATUS)"
fi

# Get download details
info "Fetching download details..."
DETAILS=$(curl -s "$SERVER_URL/downloads/$DOWNLOAD_ID" \
  -H "Authorization: Bearer $API_KEY")

SIZE=$(echo $DETAILS | grep -o '"size":[0-9]*' | cut -d':' -f2)
SHA256=$(echo $DETAILS | grep -o '"sha256":"[^"]*"' | cut -d'"' -f4)

success "Download details:"
echo "  - Download ID: $DOWNLOAD_ID"
echo "  - Status: $STATUS"
echo "  - Size: $(numfmt --to=iec-i --suffix=B $SIZE 2>/dev/null || echo $SIZE bytes)"
echo "  - SHA256: $SHA256"

# Get artifact URL
info "Fetching artifact URL..."
ARTIFACT_RESPONSE=$(curl -s "$SERVER_URL/download/$DOWNLOAD_ID/artifacts" \
  -H "Authorization: Bearer $API_KEY")

ARTIFACT_URL=$(echo $ARTIFACT_RESPONSE | grep -o '"artifactUrl":"[^"]*"' | sed 's/"artifactUrl":"//;s/".*//')

if [ -z "$ARTIFACT_URL" ]; then
  error "Failed to get artifact URL"
fi

success "Artifact URL obtained"

# Verify we can access the artifact
info "Verifying artifact accessibility..."
curl -s -I "$ARTIFACT_URL" | head -n 1 | grep "200" > /dev/null
if [ $? -ne 0 ]; then
  error "Artifact is not accessible"
fi

success "Artifact is accessible"

echo ""
echo -e "${GREEN}=== E2E Test Passed ===${NC}"
echo ""
echo "Summary:"
echo "  - Client: $CLIENT_ID"
echo "  - Download ID: $DOWNLOAD_ID"
echo "  - File Size: $(numfmt --to=iec-i --suffix=B $SIZE 2>/dev/null || echo $SIZE bytes)"
echo "  - Status: verified"
echo ""
echo "You can download the file using:"
echo "  curl -o downloaded_file.bin '$ARTIFACT_URL'"
