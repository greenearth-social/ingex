#!/usr/bin/env python3
"""
Health check for jetstream-ingest on Cloud Run, built for verifying a deploy.

By default it looks at the revision currently serving traffic: it compares the
window since that revision became ready against an equally long window just
before it (the previous revision's steady state), and checks logs for errors
and container restarts since the cutover. Run it a few minutes after a deploy
finishes; with less than ~3 minutes of data it reports TOO EARLY rather than
guessing.

Checks:
  - the latest revision is Ready and serving 100% of traffic
  - error logs on the serving revision have stopped. The app logs errors as
    plain text ("[ERROR] ...") with no severity, so both that and
    severity>=ERROR are matched. A few at the cutover are normal — the old and
    new revisions overlap briefly and like-count updates can hit an ES 409
    version conflict — so it fails only on errors in the last 3 minutes, or
    more than MAX_CUTOVER_ERRORS since the window start
  - no container exits, panics or OOM kills on the serving revision. Only the
    serving revision counts: the outgoing one logs "Container called exit(0)"
    as it drains after a cutover, which is the deploy working, not a crash.
    Lines from other revisions are listed for context but do not fail the check
  - jetstream.state_update_error_count is zero (cursor persistence to GCS)
  - jetstream.inbound_count over the last 3 minutes is at least half the
    baseline rate — a stalled websocket reads as a collapse here
  - like_counts.written_count is reported, not judged: it is bursty by design
    (counts are cached and flushed on a timer, and the outgoing revision flushes
    its cache at shutdown), so a spike around the cutover is expected

Exit codes, so an agent can branch on the result:
  0  HEALTHY
  1  UNHEALTHY — at least one check failed; the reasons are printed
  2  TOO EARLY — not enough post-deploy data yet; wait and re-run

Usage:
  # After `scripts/deploy.sh jetstream --environment stage` returns, wait
  # ~3 minutes, then:
  ./scripts/jetstream_health.py --environment stage

  # General health over the last 30 minutes, with no deploy comparison
  ./scripts/jetstream_health.py --environment prod --minutes 30

  # Compare against an explicit cutover time instead of the revision's
  ./scripts/jetstream_health.py --environment prod --since 2026-11-13T01:36:08Z

Needs an authenticated gcloud CLI with read access to Cloud Run, Cloud
Monitoring and Cloud Logging in the project. Standard library only.
"""

import argparse
import datetime as dt
import json
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_PROJECT = "greenearth-471522"
DEFAULT_REGION = "us-east1"

# Below this much post-cutover data the comparison is mostly noise: metrics
# export once a minute, and the first minute includes cursor catch-up.
MIN_WINDOW = dt.timedelta(minutes=3)
# The recent stretch inbound volume is judged on, and how far below baseline
# it may fall. Inbound is the firehose rate, so it barely moves minute to
# minute in steady state; half is a collapse, not a quiet period.
RECENT = dt.timedelta(minutes=3)
MIN_INBOUND_RATIO = 0.5

# Error lines the serving revision may log in total, outside the last RECENT
# minutes, before that alone fails the check. A cutover typically produces a
# handful; a crash loop or a broken dependency produces hundreds.
MAX_CUTOVER_ERRORS = 20

EXIT_HEALTHY, EXIT_UNHEALTHY, EXIT_TOO_EARLY = 0, 1, 2


def gcloud(*args: str) -> str:
    return subprocess.run(
        ["gcloud", *args], check=True, capture_output=True, text=True
    ).stdout


def rfc3339(t: dt.datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_time(s: str) -> dt.datetime:
    # Cloud Run timestamps carry nanoseconds, which fromisoformat rejects.
    s = s.replace("Z", "+00:00")
    if "." in s:
        head, frac = s.split(".", 1)
        tz = frac[frac.index("+"):] if "+" in frac else ""
        s = f"{head}{tz}"
    return dt.datetime.fromisoformat(s).astimezone(dt.timezone.utc)


def describe_service(service: str, project: str, region: str) -> dict:
    return json.loads(gcloud(
        "run", "services", "describe", service,
        "--region", region, "--project", project, "--format=json",
    ))


def revision_ready_time(revision: str, project: str, region: str) -> dt.datetime:
    rev = json.loads(gcloud(
        "run", "revisions", "describe", revision,
        "--region", region, "--project", project, "--format=json",
    ))
    for cond in rev.get("status", {}).get("conditions", []):
        if cond.get("type") == "Ready" and cond.get("lastTransitionTime"):
            return parse_time(cond["lastTransitionTime"])
    return parse_time(rev["metadata"]["creationTimestamp"])


def metric_per_minute(
    metric: str, env: str, start: dt.datetime, end: dt.datetime,
    project: str, token: str,
) -> dict[dt.datetime, float]:
    """Per-minute sums of an ingex counter for jetstream-ingest in env."""
    params = [
        ("filter",
         f'metric.type="custom.googleapis.com/ingex/{metric}"'
         f' AND metric.labels.service_namespace="{env}"'
         ' AND metric.labels.service_name="jetstream-ingest"'),
        ("interval.startTime", rfc3339(start)),
        ("interval.endTime", rfc3339(end)),
        ("aggregation.alignmentPeriod", "60s"),
        ("aggregation.perSeriesAligner", "ALIGN_DELTA"),
        ("aggregation.crossSeriesReducer", "REDUCE_SUM"),
    ]
    url = (f"https://monitoring.googleapis.com/v3/projects/{project}/timeSeries?"
           + urllib.parse.urlencode(params))
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        data = json.load(resp)
    points: dict[dt.datetime, float] = {}
    for series in data.get("timeSeries", []):
        for p in series["points"]:
            t = parse_time(p["interval"]["endTime"])
            v = p["value"]
            points[t] = points.get(t, 0.0) + float(v.get("int64Value", v.get("doubleValue", 0)))
    return points


def read_logs(
    service: str, extra: str, start: dt.datetime, project: str, limit: int = 200,
) -> list[tuple[str, str]]:
    """(revision, line) pairs for log entries matching extra since start."""
    flt = (f'resource.type="cloud_run_revision"'
           f' AND resource.labels.service_name="{service}"'
           f' AND timestamp>="{rfc3339(start)}"{extra}')
    out = gcloud(
        "logging", "read", flt, "--project", project, f"--limit={limit}",
        "--format=value(resource.labels.revision_name,timestamp,severity,textPayload,jsonPayload.message)",
    )
    entries = []
    for line in out.splitlines():
        if line.strip():
            revision, _, rest = line.partition("\t")
            entries.append((revision, rest))
    return entries


def entry_time(line: str) -> dt.datetime | None:
    try:
        return parse_time(line.split("\t", 1)[0])
    except ValueError:
        return None


def per_minute_rate(points: dict[dt.datetime, float], start: dt.datetime, end: dt.datetime) -> float | None:
    vals = [v for t, v in points.items() if start < t <= end]
    return sum(vals) / len(vals) if vals else None


def fmt(v: float | None) -> str:
    return "n/a" if v is None else f"{v:,.0f}/min"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check jetstream-ingest health, by default against the "
                    "window before the serving revision went live.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Exit codes: 0 healthy, 1 unhealthy, 2 too early (re-run in a few minutes).",
    )
    parser.add_argument("--environment", "-e", choices=["stage", "prod"], default="stage",
                        help="Environment to check (default: stage, matching deploy.sh)")
    when = parser.add_mutually_exclusive_group()
    when.add_argument("--since", metavar="RFC3339",
                      help="Cutover time to compare around (default: when the serving revision became ready)")
    when.add_argument("--minutes", type=int, metavar="N",
                      help="Plain health over the last N minutes, with no before/after comparison")
    parser.add_argument("--project", default=DEFAULT_PROJECT, help=f"GCP project (default: {DEFAULT_PROJECT})")
    parser.add_argument("--region", default=DEFAULT_REGION, help=f"Cloud Run region (default: {DEFAULT_REGION})")
    args = parser.parse_args()

    env, project, region = args.environment, args.project, args.region
    service = f"jetstream-ingest-{env}"
    now = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    problems: list[str] = []

    try:
        svc = describe_service(service, project, region)
        token = gcloud("auth", "print-access-token").strip()
    except subprocess.CalledProcessError as exc:
        print(f"gcloud failed: {exc.stderr.strip()}", file=sys.stderr)
        return EXIT_UNHEALTHY

    status = svc.get("status", {})
    latest = status.get("latestCreatedRevisionName")
    ready_rev = status.get("latestReadyRevisionName")
    traffic = [(t.get("revisionName"), t.get("percent", 0)) for t in status.get("traffic", [])]
    ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in status.get("conditions", []))
    print(f"{service}: latest={latest} ready={ready_rev} traffic={traffic}")
    if not ready:
        problems.append("service Ready condition is not True")
    if latest != ready_rev:
        problems.append(f"latest revision {latest} is not ready (serving {ready_rev})")
    if (ready_rev, 100) not in traffic:
        problems.append(f"{ready_rev} is not serving 100% of traffic")

    if args.minutes:
        start, baseline_start = now - dt.timedelta(minutes=args.minutes), None
        print(f"window: last {args.minutes} min (since {rfc3339(start)}), no baseline")
    else:
        start = parse_time(args.since) if args.since else revision_ready_time(ready_rev, project, region)
        if now - start < MIN_WINDOW:
            wait = int((MIN_WINDOW - (now - start)).total_seconds()) + 30
            print(f"TOO EARLY: cutover at {rfc3339(start)} was "
                  f"{int((now - start).total_seconds())}s ago. Re-run in ~{wait}s.")
            return EXIT_TOO_EARLY
        baseline_start = start - (now - start)
        print(f"window: since cutover {rfc3339(start)} ({int((now - start).total_seconds() // 60)} min), "
              f"baseline from {rfc3339(baseline_start)}")

    fetch_from = baseline_start or start
    try:
        inbound = metric_per_minute("jetstream.inbound_count", env, fetch_from, now, project, token)
        likes = metric_per_minute("like_counts.written_count", env, fetch_from, now, project, token)
        state_errors = metric_per_minute("jetstream.state_update_error_count", env, start, now, project, token)
    except urllib.error.URLError as exc:
        print(f"Cloud Monitoring query failed: {exc}", file=sys.stderr)
        return EXIT_UNHEALTHY

    recent_inbound = per_minute_rate(inbound, now - RECENT, now)
    if baseline_start:
        base_inbound = per_minute_rate(inbound, baseline_start, start)
        print(f"inbound events:     baseline {fmt(base_inbound)}, since cutover "
              f"{fmt(per_minute_rate(inbound, start, now))}, last 3 min {fmt(recent_inbound)}")
        print(f"like counts written: baseline {fmt(per_minute_rate(likes, baseline_start, start))}, "
              f"since cutover {fmt(per_minute_rate(likes, start, now))}, "
              f"last 3 min {fmt(per_minute_rate(likes, now - RECENT, now))} (bursty; not judged)")
        if not recent_inbound:
            problems.append("no inbound events in the last 3 minutes")
        elif base_inbound and recent_inbound < MIN_INBOUND_RATIO * base_inbound:
            problems.append(f"inbound rate {fmt(recent_inbound)} is under half the baseline {fmt(base_inbound)}")
    else:
        print(f"inbound events:      window {fmt(per_minute_rate(inbound, start, now))}, last 3 min {fmt(recent_inbound)}")
        print(f"like counts written: window {fmt(per_minute_rate(likes, start, now))} (bursty; not judged)")
        if not recent_inbound:
            problems.append("no inbound events in the last 3 minutes")

    state_error_total = sum(state_errors.values())
    print(f"state update errors: {state_error_total:.0f}")
    if state_error_total:
        problems.append(f"{state_error_total:.0f} state update errors (cursor not persisting to GCS)")

    try:
        errors = read_logs(service, ' AND (severity>=ERROR OR textPayload:"[ERROR]")', start, project, limit=500)
        restarts = read_logs(
            service,
            ' AND (textPayload=~"(?i)(container called exit|container terminated|exit code|'
            'memory limit .* exceeded|panic:)" OR jsonPayload.message=~"(?i)panic:")',
            start, project,
        )
    except subprocess.CalledProcessError as exc:
        print(f"Cloud Logging query failed: {exc.stderr.strip()}", file=sys.stderr)
        return EXIT_UNHEALTHY
    for name, entries in [("error logs", errors), ("restart/crash logs", restarts)]:
        serving = [line for rev, line in entries if rev == ready_rev]
        other = [f"{rev} {line}" for rev, line in entries if rev != ready_rev]
        print(f"{name}: {len(serving)} on {ready_rev}"
              + (f", {len(other)} on other revisions (not judged)" if other else ""))
        for line in (serving + other)[:5]:
            print(f"    {line[:240]}")
        if not serving:
            continue
        if name == "error logs":
            recent = [line for line in serving if (t := entry_time(line)) and t >= now - RECENT]
            if recent:
                problems.append(f"{len(recent)} error logs on {ready_rev} in the last 3 minutes")
            elif len(serving) > MAX_CUTOVER_ERRORS:
                problems.append(f"{len(serving)} error logs on {ready_rev} since {rfc3339(start)}")
            else:
                print("    (stopped before the last 3 minutes; treated as cutover noise)")
        else:
            problems.append(f"{len(serving)} {name} on {ready_rev} since {rfc3339(start)}")

    if problems:
        print("\nUNHEALTHY:")
        for p in problems:
            print(f"  - {p}")
        return EXIT_UNHEALTHY
    print("\nHEALTHY")
    return EXIT_HEALTHY


if __name__ == "__main__":
    sys.exit(main())
