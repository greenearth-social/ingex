#!/usr/bin/env python3
"""
What share of the likes we ingest come from labeler-flagged accounts?

Samples `app.bsky.feed.like` events off the public Jetstream firehose — the
same source `jetstream_ingest` consumes — and scores each liker's DID against
a labeler DID set built by `enumerate_labeler.py`. Answers the question
"if we skipped likes from flagged accounts, how many would we skip?"

The like path matters more than the post path here. Likes decide
`posts_recent_quality` membership and feed two-tower user history, and a bot
farm supplies its own likes, so `like_count >= 20` promotes coordinated
accounts into the retrieval corpus rather than filtering them out.

Sampling caveat: this is a live window, so it measures the steady state. It
will *understate* the benefit if inauthentic likes arrive in bursts — a farm
dumping thousands of likes over a few minutes is exactly the pattern a short
sample is worst at catching. Treat the result as a floor, and prefer a long
window or several spread across the day.

Usage:
  python jetstream_like_sample.py --dids skywatch_dids.json --seconds 300
  python jetstream_like_sample.py --dids skywatch_dids.json --limit 200000 \
      --values platform-manipulation,suspect-inauthentic,spam

Dependencies: websockets (already required for jetstream ingest work).
"""

import argparse
import asyncio
import collections
import json
import sys
import time

import websockets

JETSTREAM = ("wss://jetstream2.us-east.bsky.network/subscribe"
             "?wantedCollections=app.bsky.feed.like")


def load_did_set(path, values):
    with open(path) as fh:
        payload = json.load(fh)
    labels = payload.get("labels") or {}
    keep = {v for v in values.split(",")} if values else None
    by_did = collections.defaultdict(set)
    for val, dids in labels.items():
        if keep is not None and val not in keep:
            continue
        for did in dids:
            by_did[did].add(val)
    print(f"loaded {len(by_did):,} flagged DIDs from {payload.get('labeler')}",
          file=sys.stderr)
    return by_did


async def sample(args, flagged):
    total = flagged_likes = 0
    by_val = collections.Counter()
    liker_counts = collections.Counter()
    flagged_likers = set()
    start = time.time()

    async with websockets.connect(JETSTREAM, max_size=None) as ws:
        print("connected to jetstream, sampling likes ...", file=sys.stderr)
        while True:
            if args.seconds and time.time() - start >= args.seconds:
                break
            if args.limit and total >= args.limit:
                break
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
            except asyncio.TimeoutError:
                print("  no events for 30s, stopping", file=sys.stderr)
                break

            try:
                event = json.loads(raw)
            except ValueError:
                continue
            commit = event.get("commit") or {}
            if commit.get("operation") != "create":
                continue
            if commit.get("collection") != "app.bsky.feed.like":
                continue

            did = event.get("did")
            if not did:
                continue
            total += 1
            liker_counts[did] += 1
            labels = flagged.get(did)
            if labels:
                flagged_likes += 1
                flagged_likers.add(did)
                for val in labels:
                    by_val[val] += 1

            if total and total % 25000 == 0:
                pct = 100.0 * flagged_likes / total
                print(f"  {total:,} likes, {flagged_likes:,} flagged ({pct:.2f}%)",
                      file=sys.stderr)

    return total, flagged_likes, by_val, liker_counts, flagged_likers, time.time() - start


def main():
    p = argparse.ArgumentParser(
        description="Measure the flagged share of live like traffic"
    )
    p.add_argument("--dids", required=True,
                   help="JSON from enumerate_labeler.py")
    p.add_argument("--values",
                   help="comma-separated label values to count (default: all in file)")
    p.add_argument("--seconds", type=float, default=300,
                   help="how long to sample (default: 300)")
    p.add_argument("--limit", type=int, default=0,
                   help="stop after this many likes (0 = no limit)")
    args = p.parse_args()

    flagged = load_did_set(args.dids, args.values)
    total, hits, by_val, liker_counts, flagged_likers, elapsed = asyncio.run(
        sample(args, flagged)
    )

    if not total:
        print("no likes sampled", file=sys.stderr)
        return 1

    print(f"\n{'=' * 66}")
    print(f"jetstream like sample: {total:,} likes over {elapsed:.0f}s "
          f"({total / max(elapsed, 1):.0f}/s)")
    print(f"{'=' * 66}")
    print(f"  flagged likes      {hits:>9,}   {100.0 * hits / total:5.2f}%")
    print(f"  distinct likers    {len(liker_counts):>9,}")
    print(f"  flagged likers     {len(flagged_likers):>9,}"
          f"   {100.0 * len(flagged_likers) / len(liker_counts):5.2f}%")

    if by_val:
        print(f"\n  by label (a liker may carry several):")
        for val, count in by_val.most_common(15):
            print(f"    {count:>8,}  {100.0 * count / total:5.2f}%  {val}")

    # Bot farms concentrate volume, so per-liker rate is the tell.
    if flagged_likers:
        flagged_rate = hits / len(flagged_likers)
        clean_likers = len(liker_counts) - len(flagged_likers)
        clean_rate = (total - hits) / max(clean_likers, 1)
        print(f"\n  likes per liker in window: flagged {flagged_rate:.2f} "
              f"vs unflagged {clean_rate:.2f}")

    print("\n  Steady-state only: a short window understates burst behaviour,")
    print("  which is exactly how coordinated like floods arrive.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
