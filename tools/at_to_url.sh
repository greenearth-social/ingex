#!/bin/bash

# Convert AT Protocol URI to Bluesky post URL
# Usage: ./at_to_url.sh "at://did:plc:xyz123.../app.bsky.feed.post/abc123..."

if [ $# -eq 0 ]; then
    echo "Error: No AT URI provided"
    echo "Usage: $0 <at-uri>"
    echo "Example: $0 'at://did:plc:xyz123abc/app.bsky.feed.post/3km5abc123'"
    exit 1
fi

AT_URI="$1"

# Validate it's an AT URI
if [[ ! "$AT_URI" =~ ^at:// ]]; then
    echo "Error: Invalid AT URI format. Must start with 'at://'"
    exit 1
fi

# Extract DID and post ID using regex
if [[ "$AT_URI" =~ at://([^/]+)/app\.bsky\.feed\.post/([^/]+) ]]; then
    DID="${BASH_REMATCH[1]}"
    POST_ID="${BASH_REMATCH[2]}"
    
    # Construct Bluesky URL
    BLUESKY_URL="https://bsky.app/profile/${DID}/post/${POST_ID}"
    
    echo "$BLUESKY_URL"
else
    echo "Error: Could not parse AT URI. Expected format: at://DID/app.bsky.feed.post/POST_ID"
    exit 1
fi
