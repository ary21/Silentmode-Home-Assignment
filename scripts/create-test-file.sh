#!/bin/bash
set -e

echo "Creating test file (100MB)..."

# Get the target size (default 100MB)
SIZE_MB=${1:-100}

# Generate random file
dd if=/dev/urandom of="$HOME/file_to_download.txt" bs=1M count=$SIZE_MB 2>/dev/null

echo "Test file created: $HOME/file_to_download.txt"
echo "Size: $(du -h "$HOME/file_to_download.txt" | cut -f1)"
