#!/usr/bin/env python3
"""
ES statistics for the Green Earth ingex pipeline.

Reports:
  - post_creates/sec  (rate distribution over the query window)
  - posts_with_video/sec
  - video file size distribution  (Bluesky embed.video has no duration field)

Required env vars:
  GE_ELASTICSEARCH_URL      e.g. https://my-cluster.es.io:9243
  GE_ELASTICSEARCH_API_KEY  raw API key value

Usage:
  python es_stats.py
  python es_stats.py --window 7d --interval 5m

Dependencies: requests  (pip install requests)
"""

import argparse
import os
import sys

import requests


def es_search(url: str, api_key: str, index: str, body: dict) -> dict:
    resp = requests.post(
        f"{url}/{index}/_search",
        headers={
            "Authorization": f"ApiKey {api_key}",
            "Content-Type": "application/json",
        },
        json=body,
        timeout=60,
        verify=False,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        import json
        print(f"ES error: {json.dumps(data['error'], indent=2)}", file=sys.stderr)
        sys.exit(1)
    return data


def _percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    idx = min(int(len(sorted_values) * p), len(sorted_values) - 1)
    return sorted_values[idx]


def rate_stats(buckets: list[dict], interval_secs: int) -> dict:
    # Drop the last bucket — it covers a period still in progress.
    complete = buckets[:-1] if len(buckets) > 1 else buckets
    rates = sorted(b["doc_count"] / interval_secs for b in complete)
    if not rates:
        return {"mean": 0.0, "p50": 0.0, "p95": 0.0, "min": 0.0, "max": 0.0}
    return {
        "mean": sum(rates) / len(rates),
        "p50": _percentile(rates, 0.50),
        "p95": _percentile(rates, 0.95),
        "min": rates[0],
        "max": rates[-1],
    }


def print_rate(label: str, stats: dict) -> None:
    print(f"\n{label}")
    print(f"  mean {stats['mean']:.2f}/s   p50 {stats['p50']:.2f}/s   "
          f"p95 {stats['p95']:.2f}/s   min {stats['min']:.2f}/s   max {stats['max']:.2f}/s")


def mb(b: float | None) -> str:
    return f"{b / 1e6:7.1f} MB" if b is not None else "    n/a   "


INTERVAL_SECS = {"1m": 60, "5m": 300, "10m": 600, "1h": 3600, "6h": 21600, "1d": 86400}


def main() -> None:
    parser = argparse.ArgumentParser(description="ES statistics for Green Earth ingex")
    parser.add_argument("--window", default="24h",
                        help="Query window in ES date-math (default: 24h)")
    parser.add_argument("--interval", default="1m", choices=sorted(INTERVAL_SECS),
                        help="Histogram bucket interval (default: 1m)")
    args = parser.parse_args()

    url = os.environ.get("GE_ELASTICSEARCH_URL", "").rstrip("/")
    api_key = os.environ.get("GE_ELASTICSEARCH_API_KEY", "")
    if not url or not api_key:
        print("GE_ELASTICSEARCH_URL and GE_ELASTICSEARCH_API_KEY must be set", file=sys.stderr)
        sys.exit(1)

    interval_secs = INTERVAL_SECS[args.interval]
    time_filter = {"range": {"created_at": {"gte": f"now-{args.window}", "lte": "now"}}}
    histogram_agg = {
        "over_time": {
            "date_histogram": {
                "field": "created_at",
                "fixed_interval": args.interval,
                "min_doc_count": 0,
            }
        }
    }

    print(f"window={args.window}  bucket={args.interval}  index=posts-*")

    # ── post_creates/sec ────────────────────────────────────────────────────
    result = es_search(url, api_key, "posts-*", {
        "size": 0,
        "track_total_hits": True,
        "query": time_filter,
        "aggs": histogram_agg,
    })
    total_posts = result["hits"]["total"]["value"]
    buckets = result["aggregations"]["over_time"]["buckets"]
    print_rate(
        f"post_creates/sec  ({total_posts:,} posts in last {args.window})",
        rate_stats(buckets, interval_secs),
    )

    # ── posts_with_video/sec ────────────────────────────────────────────────
    result = es_search(url, api_key, "posts-*", {
        "size": 0,
        "track_total_hits": True,
        "query": {"bool": {"must": [time_filter, {"term": {"contains_video": True}}]}},
        "aggs": histogram_agg,
    })
    total_video = result["hits"]["total"]["value"]
    video_pct = 100 * total_video / max(total_posts, 1)
    buckets = result["aggregations"]["over_time"]["buckets"]
    print_rate(
        f"posts_with_video/sec  ({total_video:,} posts, {video_pct:.1f}% of total)",
        rate_stats(buckets, interval_secs),
    )

    # ── video file size distribution ────────────────────────────────────────
    # Bluesky's app.bsky.embed.video lexicon does not include a duration field,
    # so file size is the best available proxy for video length/complexity.
    result = es_search(url, api_key, "posts-*", {
        "size": 0,
        "query": {"term": {"contains_video": True}},
        "aggs": {
            "video_media": {
                "nested": {"path": "media"},
                "aggs": {
                    "video_items": {
                        "filter": {"term": {"media.media_type": "video"}},
                        "aggs": {
                            "size_stats": {"stats": {"field": "media.size"}},
                            "size_percentiles": {
                                "percentiles": {
                                    "field": "media.size",
                                    "percents": [10, 25, 50, 75, 90, 95, 99],
                                }
                            },
                        },
                    }
                },
            }
        },
    })
    agg = result["aggregations"]["video_media"]["video_items"]
    n = agg["doc_count"]
    s = agg["size_stats"]
    p = agg["size_percentiles"]["values"]

    print(f"\nvideo file size distribution  ({n:,} video items, all time)")
    print(f"  min  {mb(s['min'])}    p50  {mb(p.get('50.0'))}")
    print(f"  p10  {mb(p.get('10.0'))}    p75  {mb(p.get('75.0'))}")
    print(f"  p25  {mb(p.get('25.0'))}    p90  {mb(p.get('90.0'))}")
    print(f"  mean {mb(s['avg'])}    p95  {mb(p.get('95.0'))}")
    print(f"  max  {mb(s['max'])}    p99  {mb(p.get('99.0'))}")


if __name__ == "__main__":
    main()
