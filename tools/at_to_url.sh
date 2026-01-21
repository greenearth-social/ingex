#!/bin/bash

# Convert AT Protocol URI to Bluesky post URL
# Usage: ./at_to_url.sh "at://did:plc:xyz123.../app.bsky.feed.post/abc123..."
# With media: ./at_to_url.sh "at://did:plc:xyz123..." --media-ids "id1:image/jpeg,id2:video/mp4"

show_usage() {
    echo "Usage: $0 <at-uri> [--media-ids <id:mimetype,...>]"
    echo "Example: $0 'at://did:plc:xyz123abc/app.bsky.feed.post/3km5abc123'"
    echo "With media: $0 'at://did:plc:xyz123abc/app.bsky.feed.post/3km5abc123' --media-ids 'bafkreiabc:image/jpeg,bafkreixyz:video/mp4'"
    echo ""
    echo "Media URLs:"
    echo "  Images: https://cdn.bsky.app/img/feed_thumbnail/plain/{did}/{id}@{ext}"
    echo "  Videos: https://video.bsky.app/watch/{did}/{id}/720p/video.m3u8 (HLS playlist)"
    echo ""
    echo "To download video as MP4: ffmpeg -i '<video_url>' -c copy output.mp4"
}

if [ $# -eq 0 ]; then
    echo "Error: No AT URI provided"
    show_usage
    exit 1
fi

AT_URI="$1"
shift

MEDIA_IDS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --media-ids)
            MEDIA_IDS="$2"
            shift 2
            ;;
        *)
            echo "Error: Unknown option $1"
            show_usage
            exit 1
            ;;
    esac
done

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

    # Output media URLs if provided
    if [ -n "$MEDIA_IDS" ]; then
        IFS=',' read -ra MEDIA_ARRAY <<< "$MEDIA_IDS"
        for media_entry in "${MEDIA_ARRAY[@]}"; do
            # Parse id:mimetype format
            media_id="${media_entry%%:*}"
            mimetype="${media_entry#*:}"

            # Check if it's a video or image based on mimetype
            if [[ "$mimetype" == video/* ]]; then
                # Video: use video.bsky.app with HLS playlist (720p default)
                # URL-encode the DID (replace : with %3A)
                ENCODED_DID="${DID//:/%3A}"
                MEDIA_URL="https://video.bsky.app/watch/${ENCODED_DID}/${media_id}/720p/video.m3u8"
            else
                # Image: use cdn.bsky.app with extension
                extension="${mimetype##*/}"
                MEDIA_URL="https://cdn.bsky.app/img/feed_thumbnail/plain/${DID}/${media_id}@${extension}"
            fi
            echo "$MEDIA_URL"
        done
    fi
else
    echo "Error: Could not parse AT URI. Expected format: at://DID/app.bsky.feed.post/POST_ID"
    exit 1
fi
