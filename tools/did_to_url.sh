#!/bin/bash

# Convert a DID to a Bluesky profile URL
# Usage: ./did_to_url.sh "did:plc:xyz123..."

show_usage() {
    echo "Usage: $0 <did>"
    echo "Example: $0 'did:plc:xyz123abc'"
}

if [ $# -eq 0 ]; then
    echo "Error: No DID provided"
    show_usage
    exit 1
fi

DID="$1"

# Validate it's a DID
if [[ ! "$DID" =~ ^did: ]]; then
    echo "Error: Invalid DID format. Must start with 'did:'"
    exit 1
fi

echo "https://bsky.app/profile/${DID}"
