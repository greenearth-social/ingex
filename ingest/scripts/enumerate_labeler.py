#!/usr/bin/env python3
"""
Pull a labeler's entire account-label set, so it can be applied without a post.

This is the mechanism for using labelers on the **like** path. Labels attach to
a DID, not to a post — `queryLabels` returns `uri: "did:plc:..."` for
account-level labels — so a maintained DID set filters likes just as well as
posts, and a like record carries its author's DID. Nothing about a post is
needed to make the decision.

Two transports, both confirmed working against ozone.skywatch.blue:

  queryLabels?uriPatterns=*   paginated by an integer cursor, plain HTTP,
                              ~630 labels/sec. What this script uses.
  subscribeLabels?cursor=0    websocket, replays the labeler's whole history
                              then tails live (HTTP/1.1 upgrade required;
                              over HTTP/2 the endpoint 404s).

`queryLabels` is the right tool for a periodic rebuild and needs no websocket
client. `subscribeLabels` is the better long-run home, because jetstream_ingest
is already a cursor-managing websocket consumer and a second subscription fits
that shape exactly.

Both are cursor-based, so incremental updates are cheap: keep the last cursor,
resume from it, and only new labels come back. Retractions arrive as records
with `neg: true` and must be applied, or the set only ever grows.

Per-DID batch lookups are the wrong approach at this scale — aimod.social
starts returning 403 partway through a 45k-author sweep, while enumerating the
same labeler wholesale is both faster and gentler.

Usage:
  python enumerate_labeler.py --labeler skywatch.blue -o skywatch_dids.json
  python enumerate_labeler.py --labeler skywatch.blue -o out.json \
      --values platform-manipulation,suspect-inauthentic,spam,amplifier
  python enumerate_labeler.py --labeler skywatch.blue -o out.json \
      --resume-from out.json          # incremental update

Output is JSON: {"labeler":..., "cursor":..., "labels": {val: [did, ...]}}.

Dependencies: stdlib only.
"""

import argparse
import collections
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

PLC_DIRECTORY = "https://plc.directory/"
PUBLIC_APPVIEW = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile?actor="


def fetch_json(url, timeout=30, attempts=4):
    last = None
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return json.load(resp)
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            last = exc
            if attempt < attempts - 1:
                time.sleep(2 * (attempt + 1))
    raise last


def resolve_labeler_endpoint(actor):
    did = actor
    if not actor.startswith("did:"):
        did = fetch_json(PUBLIC_APPVIEW + urllib.parse.quote(actor)).get("did")
        if not did:
            raise ValueError(f"could not resolve handle {actor}")
    doc = fetch_json(PLC_DIRECTORY + did)
    for service in doc.get("service") or []:
        if service.get("id", "").endswith("atproto_labeler"):
            return did, service["serviceEndpoint"].rstrip("/")
    raise ValueError(f"{actor} declares no #atproto_labeler service")


def main():
    p = argparse.ArgumentParser(
        description="Enumerate a labeler's account-level labels into a DID set"
    )
    p.add_argument("--labeler", required=True, help="labeler handle or DID")
    p.add_argument("-o", "--output", required=True, help="JSON output path")
    p.add_argument("--values",
                   help="comma-separated label values to keep (default: all)")
    p.add_argument("--resume-from",
                   help="previous output file; resume from its cursor and merge")
    p.add_argument("--max-pages", type=int, default=100000,
                   help="stop after this many pages (default: effectively unlimited)")
    p.add_argument("--include-posts", action="store_true",
                   help="also keep post-level labels (at:// URIs), normally dropped")
    args = p.parse_args()

    keep = {v for v in args.values.split(",")} if args.values else None

    by_val = collections.defaultdict(set)
    cursor = None
    if args.resume_from and os.path.exists(args.resume_from):
        with open(args.resume_from) as fh:
            prior = json.load(fh)
        cursor = prior.get("cursor")
        for val, dids in (prior.get("labels") or {}).items():
            by_val[val] = set(dids)
        print(f"resuming from cursor {cursor} with "
              f"{sum(len(v) for v in by_val.values()):,} known labels", file=sys.stderr)

    did, endpoint = resolve_labeler_endpoint(args.labeler)
    print(f"{args.labeler} ({did})\n  {endpoint}", file=sys.stderr)

    seen = retracted = pages = 0
    start = time.time()
    while pages < args.max_pages:
        url = f"{endpoint}/xrpc/com.atproto.label.queryLabels?uriPatterns=*&limit=250"
        if cursor:
            url += f"&cursor={urllib.parse.quote(str(cursor))}"
        try:
            data = fetch_json(url)
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            print(f"  stopping at cursor {cursor}: {exc}", file=sys.stderr)
            break

        labels = data.get("labels") or []
        if not labels:
            break
        for label in labels:
            seen += 1
            uri, val = label.get("uri") or "", label.get("val")
            if not val:
                continue
            if not args.include_posts and not uri.startswith("did:"):
                continue
            if keep is not None and val not in keep:
                continue
            if label.get("neg"):
                by_val[val].discard(uri)
                retracted += 1
            else:
                by_val[val].add(uri)

        next_cursor = data.get("cursor")
        pages += 1
        if pages % 200 == 0:
            rate = seen / max(time.time() - start, 1e-9)
            kept = sum(len(v) for v in by_val.values())
            print(f"  {seen:,} labels ({rate:.0f}/s), {kept:,} kept, cursor {next_cursor}",
                  file=sys.stderr)
        if not next_cursor or next_cursor == cursor:
            cursor = next_cursor or cursor
            break
        cursor = next_cursor

    all_dids = set()
    for dids in by_val.values():
        all_dids |= dids

    payload = {
        "labeler": args.labeler,
        "labeler_did": did,
        "cursor": cursor,
        "labels": {v: sorted(d) for v, d in sorted(by_val.items())},
    }
    with open(args.output, "w") as fh:
        json.dump(payload, fh)

    elapsed = time.time() - start
    print(f"\nscanned {seen:,} labels in {elapsed:.0f}s ({retracted:,} retractions)",
          file=sys.stderr)
    print(f"kept {len(all_dids):,} distinct DIDs across {len(by_val)} label values",
          file=sys.stderr)
    print(f"cursor {cursor} -> {args.output}  (pass --resume-from to update)",
          file=sys.stderr)

    for val, dids in sorted(by_val.items(), key=lambda kv: -len(kv[1]))[:25]:
        print(f"  {len(dids):>8,}  {val}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
