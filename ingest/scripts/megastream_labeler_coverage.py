#!/usr/bin/env python3
"""
Would subscribing to a third-party labeler actually catch anything we ingest?

Takes the author DIDs out of megastream chunks, asks a labeler's Ozone
instance what it knows about them, and reports the share of *posts* (not just
accounts) that a subscription would let us drop.

Why this is not answerable from the megastream payload: the author profile in
`hydrated_metadata.user.labels` only carries labels from labelers the
hydrating client subscribes to — in practice just Bluesky's own
`moderation.bsky.app`. Third-party labelers have to be asked directly, which
is what this does, via `com.atproto.label.queryLabels` on the labeler's
declared `#atproto_labeler` service endpoint.

Labels are weighted by post volume, because that is the thing we would stop
indexing. A labeler that catches a handful of accounts producing thousands of
posts is worth more than one that catches thousands of quiet accounts.

Known labelers worth pointing this at (resolve the endpoint with --labeler,
which accepts a handle or DID):

  skywatch.blue                 spam, engagement-abuse, platform-manipulation,
                                bulk-following, repetitive-domain-spam-*
  labeler.hailey.at             ai-agent, spam, shopping-spam, reply-link-spam
  profile-labels.bossett.social rapidposts, onlyreplies, changedhandle, bridgy
  perisai.bsky.social           autobase, scam, impersonation, affiliator
  engagement-hacks.bsky.social  engagement-hack, content-spam

Usage:
  python megastream_labeler_coverage.py chunks/*.db.zip --labeler skywatch.blue
  python megastream_labeler_coverage.py chunks/*.db.zip --labeler skywatch.blue \
      --ignore-labels bluesky-elder,bluesky-newcomer

Dependencies: stdlib only.
"""

import argparse
import collections
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

PLC_DIRECTORY = "https://plc.directory/"
PUBLIC_APPVIEW = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile?actor="

# Labels that describe an account without accusing it of anything. Skywatch
# emits bluesky-elder/bluesky-newcomer as informational context, and counting
# them as spam would make any labeler look wildly effective.
DEFAULT_IGNORE = "bluesky-elder,bluesky-newcomer,bluesky-veteran"


def sniff_open_sqlite(path, tmpdir):
    with open(path, "rb") as fh:
        magic = fh.read(4)
    if magic[:2] == b"PK":
        with zipfile.ZipFile(path) as zf:
            names = [n for n in zf.namelist() if n.endswith(".db")]
            if not names:
                raise ValueError(f"no .db member inside {path}")
            return zf.extract(names[0], tmpdir)
    if magic == b"SQLi":
        return path
    raise ValueError(f"{path} is neither a zip nor a SQLite database")


def fetch_json(url, timeout=30, attempts=4):
    for attempt in range(attempts):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return json.load(resp)
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            if attempt == attempts - 1:
                raise
            time.sleep(1.5 * (attempt + 1))
            last = exc
    raise last


def resolve_labeler_endpoint(actor):
    """Return (did, ozone endpoint) for a labeler handle or DID."""
    did = actor
    if not actor.startswith("did:"):
        profile = fetch_json(PUBLIC_APPVIEW + urllib.parse.quote(actor))
        did = profile.get("did")
        if not did:
            raise ValueError(f"could not resolve handle {actor}")
    doc = fetch_json(PLC_DIRECTORY + did)
    for service in doc.get("service") or []:
        if service.get("id", "").endswith("atproto_labeler"):
            return did, service["serviceEndpoint"].rstrip("/")
    raise ValueError(f"{actor} declares no #atproto_labeler service")


def author_post_counts(paths, tmpdir):
    """Map author DID -> number of post creates, deduplicated by at_uri."""
    counts = collections.Counter()
    seen = set()
    for path in paths:
        print(f"reading {os.path.basename(path)} ...", file=sys.stderr)
        try:
            db = sniff_open_sqlite(path, tmpdir)
        except ValueError as exc:
            print(f"  skipped: {exc}", file=sys.stderr)
            continue
        con = sqlite3.connect(db)
        query = "SELECT at_uri, did, raw_post FROM enriched_posts"
        for at_uri, did, raw_post in con.execute(query):
            if not did or at_uri in seen:
                continue
            try:
                commit = (json.loads(raw_post).get("message") or {}).get("commit") or {}
            except (TypeError, ValueError):
                continue
            if commit.get("operation") != "create":
                continue
            seen.add(at_uri)
            counts[did] += 1
        con.close()
    return counts


def query_labels(endpoint, dids, batch_size):
    """Yield label records for the given DIDs, batched."""
    for start in range(0, len(dids), batch_size):
        batch = dids[start:start + batch_size]
        query = [("uriPatterns", d) for d in batch]
        query.append(("limit", "250"))
        url = f"{endpoint}/xrpc/com.atproto.label.queryLabels?" + urllib.parse.urlencode(query)
        try:
            data = fetch_json(url)
        except (urllib.error.URLError, TimeoutError, ValueError) as exc:
            print(f"  batch at {start} failed: {exc}", file=sys.stderr)
            continue
        for label in data.get("labels") or []:
            yield label
        if start and start % (batch_size * 40) == 0:
            print(f"  queried {start:,}/{len(dids):,} authors", file=sys.stderr)


def main():
    p = argparse.ArgumentParser(
        description="Measure what a third-party labeler would catch in ingested posts"
    )
    p.add_argument("files", nargs="+", help="megastream .db / .db.zip chunks")
    p.add_argument("--labeler", required=True,
                   help="labeler handle or DID (e.g. skywatch.blue)")
    p.add_argument("--batch-size", type=int, default=50,
                   help="DIDs per queryLabels request (default: 50)")
    p.add_argument("--ignore-labels", default=DEFAULT_IGNORE,
                   help="comma-separated informational labels to exclude from totals")
    p.add_argument("--cache-file",
                   help="save/reuse the raw label lookup, so a different "
                        "--ignore-labels can be scored without re-querying")
    args = p.parse_args()

    ignore = {v for v in args.ignore_labels.split(",") if v}

    tmpdir = tempfile.mkdtemp(prefix="labeler_coverage_")
    try:
        counts = author_post_counts(args.files, tmpdir)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    total_posts = sum(counts.values())
    dids = list(counts)
    print(f"\n{len(dids):,} distinct authors, {total_posts:,} post creates",
          file=sys.stderr)

    by_val_accounts = collections.defaultdict(set)
    if args.cache_file and os.path.exists(args.cache_file):
        print(f"reusing cached lookup {args.cache_file}", file=sys.stderr)
        with open(args.cache_file) as fh:
            for val, accounts in json.load(fh).items():
                by_val_accounts[val] = set(accounts)
    else:
        did, endpoint = resolve_labeler_endpoint(args.labeler)
        print(f"labeler {args.labeler} ({did})\n  endpoint {endpoint}", file=sys.stderr)
        for label in query_labels(endpoint, dids, args.batch_size):
            if label.get("neg"):
                continue
            val, uri = label.get("val"), label.get("uri")
            if val and uri in counts:
                by_val_accounts[val].add(uri)
        if args.cache_file:
            with open(args.cache_file, "w") as fh:
                json.dump({v: sorted(a) for v, a in by_val_accounts.items()}, fh)

    print(f"\n{'=' * 70}")
    print(f"{args.labeler} coverage of {total_posts:,} ingested posts")
    print(f"{'=' * 70}")
    if not by_val_accounts:
        print("  no labels returned for any ingested author")
        return 0

    rows = []
    for val, accounts in by_val_accounts.items():
        posts = sum(counts[a] for a in accounts)
        rows.append((posts, len(accounts), val))
    rows.sort(reverse=True)

    actionable_accounts = set()
    for posts, n_accounts, val in rows:
        note = "  (informational, excluded)" if val in ignore else ""
        if val not in ignore:
            actionable_accounts |= by_val_accounts[val]
        print(f"  {posts:>7,} posts  {100.0 * posts / max(total_posts, 1):5.2f}%"
              f"  {n_accounts:>6,} accts  {val}{note}")

    actionable_posts = sum(counts[a] for a in actionable_accounts)
    print(f"  {'-' * 62}")
    print(f"  {actionable_posts:>7,} posts  "
          f"{100.0 * actionable_posts / max(total_posts, 1):5.2f}%"
          f"  {len(actionable_accounts):>6,} accts  UNION (excluding informational)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
