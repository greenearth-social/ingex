#!/usr/bin/env python3
"""Render a list of AT URI posts as a Bluesky feed in the terminal.

For internal debugging of feed generation pipelines. Shows post content,
author info, like/repost/reply counts, embeds (as text descriptions), and
content labels. No authentication required (uses the public API).

Usage:
    # Pass URIs as command line arguments
    python render_feed.py at://did:plc:xxx/app.bsky.feed.post/yyy [...]

    # Pass URIs on stdin (one per line)
    echo "at://did:plc:xxx/app.bsky.feed.post/yyy" | python render_feed.py
    cat uris.txt | python render_feed.py

    # Import and call from Python
    from render_feed import render_feed
    render_feed(["at://did:plc:xxx/app.bsky.feed.post/yyy"])

Dependencies:
    pip install httpx rich
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from typing import Any, Optional

import httpx
from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from rich import box

PUBLIC_API = "https://public.api.bsky.app"
POSTS_ENDPOINT = f"{PUBLIC_API}/xrpc/app.bsky.feed.getPosts"
MAX_URIS_PER_REQUEST = 25

console = Console()


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------


def fetch_posts(uris: list[str]) -> list[dict[str, Any]]:
    """Fetch post views from the Bluesky public API.

    Args:
        uris: AT Protocol URIs (``at://did/app.bsky.feed.post/rkey``).

    Returns:
        List of post-view dicts in the order returned by the API.
    """
    posts: list[dict[str, Any]] = []
    with httpx.Client(timeout=30) as client:
        for i in range(0, len(uris), MAX_URIS_PER_REQUEST):
            batch = uris[i : i + MAX_URIS_PER_REQUEST]
            resp = client.get(
                POSTS_ENDPOINT,
                params=[("uris", u) for u in batch],
            )
            resp.raise_for_status()
            posts.extend(resp.json().get("posts", []))
    return posts


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _relative_time(iso_str: str) -> str:
    """Turn an ISO-8601 timestamp into a compact relative time string."""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        delta = datetime.now(timezone.utc) - dt
        secs = delta.total_seconds()
        if secs < 0:
            return "just now"
        if secs < 60:
            return "just now"
        if secs < 3_600:
            return f"{int(secs / 60)}m ago"
        if secs < 86_400:
            return f"{int(secs / 3_600)}h ago"
        if delta.days < 30:
            return f"{delta.days}d ago"
        return dt.strftime("%b %d, %Y")
    except Exception:
        return iso_str


def _compact_count(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 10_000:
        return f"{n / 1_000:.0f}K"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


# ---------------------------------------------------------------------------
# Embed rendering (text-only summaries)
# ---------------------------------------------------------------------------


def _format_embed(embed: dict[str, Any] | None) -> Text | None:
    """Return a ``rich.Text`` summary of a post embed, or *None*."""
    if not embed:
        return None

    etype = embed.get("$type", "")
    t = Text()

    # --- images ---
    if "images" in etype:
        images = embed.get("images", [])
        n = len(images)
        t.append(f"[{n} image{'s' if n != 1 else ''}]", style="dim yellow")
        for img in images:
            alt = (img.get("alt") or "").strip()
            if alt:
                t.append(f"\n  alt: {alt}", style="dim")

    # --- video ---
    elif "video" in etype:
        t.append("[video]", style="dim yellow")

    # --- external link ---
    elif "external" in etype:
        ext = embed.get("external", {})
        title = ext.get("title", "")
        uri = ext.get("uri", "")
        desc = ext.get("description", "")
        t.append("[link] ", style="dim yellow")
        if title:
            t.append(title, style="dim bold")
        if uri:
            t.append(f"\n  {uri}", style="dim underline")
        if desc:
            t.append(f"\n  {desc}", style="dim")

    # --- quote post (recordWithMedia or record) ---
    elif "record" in etype:
        inner = embed.get("record", {})
        # recordWithMedia wraps another record + media
        if inner.get("$type", "").endswith("#viewRecord"):
            _append_quoted(t, inner)
        elif "record" in inner:
            _append_quoted(t, inner.get("record", {}))
        else:
            _append_quoted(t, inner)

        # If recordWithMedia, also show the media part
        media = embed.get("media")
        if media:
            media_text = _format_embed(media)
            if media_text:
                t.append("\n  ")
                t.append_text(media_text)

    return t if t.plain.strip() else None


def _append_quoted(t: Text, rec: dict[str, Any]) -> None:
    """Append a one-line summary of a quoted record to *t*."""
    author = rec.get("author", {})
    value = rec.get("value", {})
    handle = author.get("handle", "")
    name = author.get("displayName", "")
    body = value.get("text", "") if isinstance(value, dict) else ""

    t.append("quote ", style="dim yellow")
    if name:
        t.append(name, style="dim bold")
        t.append(f" @{handle}", style="dim")
    elif handle:
        t.append(f"@{handle}", style="dim bold")
    if body:
        # Collapse newlines for a compact quote
        body = body.replace("\n", " ")
        t.append(f"\n  \u201c{body}\u201d", style="dim italic")


# ---------------------------------------------------------------------------
# Post rendering
# ---------------------------------------------------------------------------


def _render_post(post: dict[str, Any], index: int, total: int) -> Panel:
    """Build a ``rich.Panel`` for a single post view."""
    author = post.get("author", {})
    record = post.get("record", {})

    display_name = author.get("displayName", "")
    handle = author.get("handle", "unknown")
    text_body = record.get("text", "")
    created_at = record.get("createdAt", "")

    like_count = post.get("likeCount", 0)
    repost_count = post.get("repostCount", 0)
    reply_count = post.get("replyCount", 0)
    quote_count = post.get("quoteCount", 0)

    # --- author line ---
    author_line = Text()
    if display_name:
        author_line.append(display_name, style="bold white")
        author_line.append(f"  @{handle}", style="dim")
    else:
        author_line.append(f"@{handle}", style="bold white")
    author_line.append(f"  {_relative_time(created_at)}", style="dim cyan")

    # Reply indicator
    if record.get("reply"):
        author_line.append("  \u21a9 reply", style="dim magenta")

    # --- content labels / warnings ---
    labels = post.get("labels", [])
    label_text: Optional[Text] = None
    if labels:
        label_text = Text()
        label_text.append("labels: ", style="bold red")
        label_text.append(
            ", ".join(lb.get("val", "?") for lb in labels), style="red"
        )

    # --- embed ---
    embed_text = _format_embed(post.get("embed"))

    # --- stats ---
    stats = Text()
    stats.append("\u2665 ", style="red")
    stats.append(_compact_count(like_count))
    stats.append("   \u21bb ", style="green")
    stats.append(_compact_count(repost_count))
    stats.append("   \u21a9 ", style="cyan")
    stats.append(_compact_count(reply_count))
    if quote_count:
        stats.append("   \u275d ", style="yellow")
        stats.append(_compact_count(quote_count))

    # --- assemble ---
    content = Text()
    content.append_text(author_line)

    if label_text:
        content.append("\n")
        content.append_text(label_text)

    content.append("\n\n")
    content.append(text_body)

    if embed_text:
        content.append("\n\n")
        content.append_text(embed_text)

    content.append("\n\n")
    content.append_text(stats)

    uri = post.get("uri", "")
    title = f"[dim]{index}/{total}[/dim]  [dim cyan]{uri}[/dim cyan]"

    return Panel(
        content,
        title=title,
        title_align="left",
        box=box.ROUNDED,
        border_style="blue",
        padding=(0, 2),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def render_feed(uris: list[str], *, file=None) -> None:
    """Fetch and display posts for the given AT URIs.

    This is the main entry point when using the module as a library::

        from render_feed import render_feed
        render_feed(["at://did:plc:xxx/app.bsky.feed.post/yyy"])

    Args:
        uris: AT Protocol URIs to render.
        file: Optional writable file object for output (defaults to stdout).
    """
    out = Console(file=file) if file else console

    if not uris:
        out.print("[yellow]No URIs provided.[/yellow]")
        return

    # Deduplicate while preserving input order.
    seen: set[str] = set()
    unique: list[str] = []
    for raw in uris:
        u = raw.strip()
        if u and u not in seen:
            seen.add(u)
            unique.append(u)

    if not unique:
        out.print("[yellow]No valid URIs provided.[/yellow]")
        return

    n = len(unique)
    out.print(f"\n[bold]Fetching {n} post{'s' if n != 1 else ''}\u2026[/bold]\n")

    try:
        posts = fetch_posts(unique)
    except httpx.HTTPStatusError as exc:
        out.print(f"[red]API error {exc.response.status_code}: {exc.response.text}[/red]")
        return
    except httpx.RequestError as exc:
        out.print(f"[red]Network error: {exc}[/red]")
        return

    if not posts:
        out.print("[yellow]No posts found for the given URIs.[/yellow]")
        return

    # Re-order posts to match the requested URI order.
    by_uri: dict[str, dict[str, Any]] = {p["uri"]: p for p in posts}
    ordered: list[dict[str, Any]] = []
    missing: list[str] = []
    for u in unique:
        if u in by_uri:
            ordered.append(by_uri[u])
        else:
            missing.append(u)

    total = len(ordered)
    for idx, post in enumerate(ordered, 1):
        out.print(_render_post(post, idx, total))

    if missing:
        out.print(
            f"\n[yellow]\u26a0 {len(missing)} URI{'s' if len(missing) != 1 else ''} "
            f"not found:[/yellow]"
        )
        for u in missing:
            out.print(f"  [dim]{u}[/dim]")

    out.print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Render Bluesky posts from AT URIs as a feed in the terminal.",
        epilog="URIs can also be piped via stdin, one per line.",
    )
    parser.add_argument(
        "uris",
        nargs="*",
        help="AT Protocol URIs (at://did/app.bsky.feed.post/rkey)",
    )
    args = parser.parse_args()

    uris: list[str] = list(args.uris or [])

    # Fall back to stdin when no positional args and stdin is a pipe.
    if not uris and not sys.stdin.isatty():
        uris = [line.strip() for line in sys.stdin if line.strip()]

    if not uris:
        parser.print_help()
        sys.exit(1)

    render_feed(uris)


if __name__ == "__main__":
    main()
