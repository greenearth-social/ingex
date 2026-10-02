#!/usr/bin/env python3
"""
How much of the megastream feed could we drop before indexing?

Reads megastream SQLite chunks and reports what share of ingested posts fall
into categories we could plausibly skip: accounts labelled bot/spam by a
labeler, adult-content labels, posts the upstream `marketing_check` classifier
scores as marketing spam, and heuristic bot signals from the author profile.

None of these signals reach Elasticsearch today. `megastream_message.go` reads
about fifteen fields out of each row and discards the rest, including the
`hydrated_metadata.user` profile (which carries `labels[]`) and every
`inferences.text.*` analyzer. So this runs against the raw chunks, not ES.

Source files come from the same requester-pays bucket megastream_ingest spools
(`graze-mega-02`, prefix `mega/`), roughly one ~115 MB chunk per five minutes.
Despite the `.db.zip` suffix, current chunks are raw SQLite; older ones are
real zips. Both are handled here, same as the spooler's own sniffing.

Usage:
  # Analyze files already on disk
  python megastream_drop_analysis.py chunks/*.db.zip

  # Fetch the N newest chunks from S3 first (needs aws CLI + credentials)
  python megastream_drop_analysis.py --s3-latest 2 --cache-dir ./chunks

A warning about `marketing_check`, measured on the 2026-08-12 23:18-23:23 UTC
chunks: it does not appear to discriminate. Posts matching obvious spam
patterns (line.me/telegram invites, promo codes, crypto) had median score
0.49 against 0.42 for everything else, and hand-reading the 0.8+ tail turned
up ordinary political and conversational posts, not marketing. Its output is
reported here because it is the only per-post spam signal upstream gives us,
but it should be validated against labelled examples before anyone drops a
single post on its say-so.

Credentials for --s3-latest come from the environment (AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION); `set -a && . ingest/.env` works.
Downloads are requester-pays, so you are paying for the transfer.

Dependencies: stdlib only (aws CLI required for --s3-latest).
"""

import argparse
import collections
import datetime as dt
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import zipfile

S3_BUCKET = "graze-mega-02"
S3_PREFIX = "mega/"

# Third-party labeler verdicts of abuse. In practice these come from exactly
# one labeler, moderation.bsky.app, and they are rare (~0.3% of posts).
DEFAULT_SPAM_LABELS = "spam,scam,impersonation,inauthentic,engagement-farming"

# Self-declared automation. `bot` is applied by the account to itself — the AT
# Protocol convention for honest bots — so it catches well-behaved automation
# and never catches a spammer pretending to be human. Counted separately from
# the labeler verdicts above because it is a different kind of claim: openly
# automated, not judged abusive by anyone.
DEFAULT_BOT_LABELS = "bot,automated,com.atproto.label.defs#bot"

# Adult-content labels. Reported separately because dropping these is a product
# call, not a data-quality one — they are not spam.
DEFAULT_ADULT_LABELS = "porn,nudity,sexual,sexual-figurative,graphic-media"

# Labels that look alarming but are not drop candidates, called out so they
# don't quietly inflate a "labelled accounts" number:
#   !no-unauthenticated  — hide from logged-out viewers; a privacy preference
#   bridged-from-*       — post mirrored from ActivityPub/web by Bridgy Fed
BENIGN_LABELS = {"!no-unauthenticated"}


def sniff_open_sqlite(path, tmpdir):
    """Return a path to a readable SQLite file, extracting it if it's a zip."""
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


def s3_latest_keys(limit, stride=1):
    """Return the newest `limit` chunk keys, taking every `stride`-th one.

    Keys sort chronologically. The archive holds ~800 chunks of ~115 MB per
    day, so covering a longer span means sampling rather than downloading
    everything: `--s3-latest 24 --s3-stride 33` spans about a day for 24
    chunks' worth of (requester-pays) transfer.
    """
    out = subprocess.run(
        [
            "aws", "s3api", "list-objects-v2",
            "--bucket", S3_BUCKET,
            "--prefix", f"{S3_PREFIX}mega_jetstream_",
            "--request-payer", "requester",
            "--query", "Contents[*].Key",
            "--output", "text",
        ],
        capture_output=True, text=True, check=True,
    )
    keys = sorted(k for k in out.stdout.split() if k.endswith(".db.zip"))
    sampled = keys[::-1][::max(stride, 1)][:limit]
    return sampled[::-1]


def s3_download(key, dest_dir):
    dest = os.path.join(dest_dir, os.path.basename(key))
    if os.path.exists(dest):
        return dest
    os.makedirs(dest_dir, exist_ok=True)
    subprocess.run(
        [
            "aws", "s3api", "get-object",
            "--bucket", S3_BUCKET,
            "--key", key,
            "--request-payer", "requester",
            dest,
        ],
        capture_output=True, text=True, check=True,
    )
    return dest


def parse_iso(value):
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def active_labels(user, now, subject_did=None):
    """Active label values on an account, split by who applied them.

    Returns (third_party, self_applied). A label whose `src` is the account's
    own DID was applied by that account to itself — that is how
    `!no-unauthenticated`, `bot`, and the bridged-from-* markers arrive — and
    carries no independent verdict. Negated and expired labels are dropped.
    """
    third_party, self_applied = [], []
    subject = subject_did or user.get("did")
    for label in user.get("labels") or []:
        if label.get("neg"):
            continue
        exp = parse_iso(label.get("exp"))
        if exp is not None and exp < now:
            continue
        val = label.get("val")
        if not val:
            continue
        src = label.get("src")
        if src and subject and src == subject:
            self_applied.append(val)
        else:
            third_party.append(val)
    return third_party, self_applied


def marketing_spam_score(inferences):
    """Highest 'Marketing Spam' probability across the post's analyzed texts.

    `inferences.text` is keyed by the JSON path of the field analyzed — the post
    body plus, when present, external embed title/description. We take the max
    so a spam link card counts even when the body reads clean.
    """
    best = None
    for analysis in (inferences.get("text") or {}).values():
        if not isinstance(analysis, dict):
            continue
        check = analysis.get("marketing_check")
        if isinstance(check, dict) and "Marketing Spam" in check:
            score = check["Marketing Spam"]
            if best is None or score > best:
                best = score
    return best


def posts_per_day(user, now):
    created = parse_iso(user.get("created_at"))
    count = user.get("posts_count")
    if created is None or not isinstance(count, int):
        return None
    age_days = max((now - created).total_seconds() / 86400.0, 1.0)
    return count / age_days


class Stats:
    def __init__(self, args):
        self.args = args
        self.spam_labels = set(args.spam_labels.split(","))
        self.bot_labels = set(args.bot_labels.split(","))
        self.adult_labels = set(args.adult_labels.split(","))

        self.rows = 0
        self.seen_uris = set()
        self.duplicate_rows = 0
        self.kinds = collections.Counter()
        self.operations = collections.Counter()

        # Population under analysis: newly created posts.
        self.creates = 0
        self.replies = 0
        self.top_level = 0

        self.no_profile = 0
        self.no_marketing_check = 0

        self.label_values = collections.Counter()
        self.self_label_values = collections.Counter()
        self.self_declared_bots = set()
        self.drop_flags = collections.Counter()
        self.any_drop = 0
        self.label_only_drop = 0
        self.spam_or_bot_accounts = set()
        self.all_accounts = set()
        self.langs = collections.Counter()

        # Raw values behind the two threshold-driven flags, kept so the report
        # can show how much the answer moves with the cutoff.
        self.marketing_scores = []
        self.post_rates = []

    def add_file(self, path, tmpdir):
        db = sniff_open_sqlite(path, tmpdir)
        con = sqlite3.connect(db)
        now = dt.datetime.now(dt.timezone.utc)
        query = "SELECT at_uri, did, raw_post, inferences FROM enriched_posts"
        for at_uri, did, raw_post, inferences in con.execute(query):
            self.add_row(at_uri, did, raw_post, inferences, now)
        con.close()

    def add_row(self, at_uri, did, raw_post, inferences, now):
        self.rows += 1
        try:
            post = json.loads(raw_post)
        except (TypeError, ValueError):
            return

        message = post.get("message") or {}
        commit = message.get("commit") or {}
        self.kinds[message.get("kind") or "(none)"] += 1
        self.operations[commit.get("operation") or "(none)"] += 1

        if commit.get("operation") != "create":
            return
        if at_uri in self.seen_uris:
            self.duplicate_rows += 1
            return
        self.seen_uris.add(at_uri)

        self.creates += 1
        record = commit.get("record") or {}
        hydrated = post.get("hydrated_metadata") or {}
        is_reply = bool(record.get("reply") or hydrated.get("parent_post"))
        if is_reply:
            self.replies += 1
        else:
            self.top_level += 1

        for lang in record.get("langs") or []:
            self.langs[lang] += 1

        if did:
            self.all_accounts.add(did)

        flags = set()

        user = hydrated.get("user")
        if not user:
            self.no_profile += 1
        else:
            third_party, self_applied = active_labels(user, now, did)
            for val in third_party:
                self.label_values[val] += 1
            for val in self_applied:
                self.self_label_values[val] += 1
            if self.spam_labels.intersection(third_party):
                flags.add("labeler_spam_verdict")
                if did:
                    self.spam_or_bot_accounts.add(did)
            if self.bot_labels.intersection(self_applied):
                flags.add("self_declared_bot")
                if did:
                    self.self_declared_bots.add(did)
            if self.adult_labels.intersection(third_party + self_applied):
                flags.add("account_label_adult")

            rate = posts_per_day(user, now)
            if rate is not None:
                self.post_rates.append(rate)
                if rate >= self.args.max_posts_per_day:
                    flags.add("heuristic_post_rate")

            followers = user.get("followers_count")
            follows = user.get("follows_count")
            if (
                isinstance(followers, int)
                and isinstance(follows, int)
                and followers < self.args.min_followers
                and follows >= self.args.follow_ratio * max(followers, 1)
            ):
                flags.add("heuristic_follow_ratio")

        try:
            inf = json.loads(inferences)
        except (TypeError, ValueError):
            inf = {}
        score = marketing_spam_score(inf)
        if score is None:
            self.no_marketing_check += 1
        else:
            self.marketing_scores.append(score)
            if score >= self.args.marketing_threshold:
                flags.add("marketing_spam")

        for flag in flags:
            self.drop_flags[flag] += 1
        if flags:
            self.any_drop += 1
        if flags.intersection(
            {"self_declared_bot", "labeler_spam_verdict", "account_label_adult"}
        ):
            self.label_only_drop += 1

    # ── reporting ────────────────────────────────────────────────────────

    def pct(self, n):
        return 100.0 * n / max(self.creates, 1)

    def report(self):
        a = self.args
        print(f"\n{'=' * 68}\ncorpus composition\n{'=' * 68}")
        print(f"  rows read                {self.rows:>10,}")
        for kind, count in self.kinds.most_common():
            print(f"    kind={kind:<20} {count:>10,}")
        for op, count in self.operations.most_common():
            print(f"    op={op:<22} {count:>10,}")
        print(f"  duplicate at_uri rows    {self.duplicate_rows:>10,}"
              "   (chunks overlap; counted once)")
        print(f"  distinct post creates    {self.creates:>10,}   <- denominator")
        print(f"    top-level              {self.top_level:>10,}"
              f"   {self.pct(self.top_level):5.1f}%")
        print(f"    replies                {self.replies:>10,}"
              f"   {self.pct(self.replies):5.1f}%")
        print(f"  distinct authors         {len(self.all_accounts):>10,}")

        print(f"\n{'=' * 68}\nsignal coverage\n{'=' * 68}")
        have_profile = self.creates - self.no_profile
        have_mc = self.creates - self.no_marketing_check
        print(f"  author profile present   {have_profile:>10,}"
              f"   {self.pct(have_profile):5.1f}%")
        print(f"  marketing_check present  {have_mc:>10,}"
              f"   {self.pct(have_mc):5.1f}%")
        print("  (posts missing a signal can never be flagged by it, so the")
        print("   percentages below are lower bounds)")

        print(f"\n{'=' * 68}\ndroppable categories (% of distinct post creates)\n{'=' * 68}")
        rows = [
            ("self-declared bot account", "self_declared_bot"),
            ("labeler spam verdict", "labeler_spam_verdict"),
            ("account labelled adult", "account_label_adult"),
            ("marketing_check spam", "marketing_spam"),
            ("heuristic: post rate", "heuristic_post_rate"),
            ("heuristic: follow ratio", "heuristic_follow_ratio"),
        ]
        for title, key in rows:
            count = self.drop_flags[key]
            print(f"  {title:<28} {count:>9,}   {self.pct(count):5.1f}%")
        print(f"  {'-' * 50}")
        print(f"  {'union (label-based only)':<28} {self.label_only_drop:>9,}"
              f"   {self.pct(self.label_only_drop):5.1f}%")
        print(f"  {'union (any of the above)':<28} {self.any_drop:>9,}"
              f"   {self.pct(self.any_drop):5.1f}%")
        print("\n  Trust the label-based union. On a two-chunk sample the")
        print("  marketing_check classifier barely separated obvious spam")
        print("  (median 0.49) from everything else (0.42), and its highest")
        print("  scores landed on ordinary political posts — see the module")
        print("  docstring. The post-rate heuristic is a rate, not a verdict:")
        print("  prolific humans and news bots both clear it.")
        print(f"\n  thresholds: marketing_check >= {a.marketing_threshold}, "
              f"posts/day >= {a.max_posts_per_day},")
        print(f"              follows >= {a.follow_ratio}x followers "
              f"with < {a.min_followers} followers")
        print(f"  labeler spam labels: {sorted(self.spam_labels)}")
        print(f"  self-declared bot labels: {sorted(self.bot_labels)}")
        print(f"  accounts with a labeler spam verdict: "
              f"{len(self.spam_or_bot_accounts):,} of {len(self.all_accounts):,}")
        print(f"  accounts self-declaring as bots:      "
              f"{len(self.self_declared_bots):,} of {len(self.all_accounts):,}")

        print(f"\n{'=' * 68}\nthreshold sensitivity\n{'=' * 68}")
        print("  marketing_check 'Marketing Spam' probability:")
        for cutoff in (0.5, 0.6, 0.7, 0.8, 0.9, 0.95):
            hits = sum(1 for s in self.marketing_scores if s >= cutoff)
            mark = "  <- current" if abs(cutoff - a.marketing_threshold) < 1e-9 else ""
            print(f"    >= {cutoff:<5} {hits:>8,}   {self.pct(hits):5.1f}%{mark}")
        print("  author lifetime posts/day:")
        for cutoff in (25, 50, 100, 200, 500):
            hits = sum(1 for r in self.post_rates if r >= cutoff)
            mark = "  <- current" if abs(cutoff - a.max_posts_per_day) < 1e-9 else ""
            print(f"    >= {cutoff:<5} {hits:>8,}   {self.pct(hits):5.1f}%{mark}")

        for title, counter in (
            ("account labels: THIRD-PARTY (another labeler's verdict)", self.label_values),
            ("account labels: SELF-APPLIED (the account's own setting)",
             self.self_label_values),
        ):
            print(f"\n{'=' * 68}\n{title}\n{'=' * 68}")
            if not counter:
                print("  (none observed)")
                continue
            for val, count in counter.most_common(20):
                note = ""
                if val in BENIGN_LABELS:
                    note = "  <- privacy preference, NOT a drop candidate"
                elif val.startswith("bridged-from-"):
                    note = "  <- bridged from another network"
                elif val in self.spam_labels:
                    note = "  <- counted as labeler spam"
                elif val in self.bot_labels:
                    note = "  <- counted as self-declared bot"
                elif val in self.adult_labels:
                    note = "  <- counted as adult"
                print(f"  {count:>8,}  {self.pct(count):5.1f}%  {val}{note}")

        print(f"\n{'=' * 68}\ndeclared languages (posts; a post may declare several)\n{'=' * 68}")
        total_lang = sum(self.langs.values())
        for lang, count in self.langs.most_common(10):
            share = 100.0 * count / max(total_lang, 1)
            print(f"  {count:>8,}  {share:5.1f}%  {lang}")
        non_en = total_lang - self.langs.get("en", 0)
        print(f"  non-'en' declared: {non_en:,} ({100.0 * non_en / max(total_lang, 1):.1f}%)")


def main():
    p = argparse.ArgumentParser(
        description="Estimate how much megastream data could be dropped before indexing"
    )
    p.add_argument("files", nargs="*", help="megastream .db / .db.zip chunks")
    p.add_argument("--s3-latest", type=int, default=0,
                   help="fetch the N newest chunks from S3 before analyzing")
    p.add_argument("--s3-stride", type=int, default=1,
                   help="take every Nth chunk when sampling, to span a longer period "
                        "for the same download volume (~800 chunks/day; default: 1)")
    p.add_argument("--cache-dir", default="./megastream_chunks",
                   help="where --s3-latest downloads land (default: ./megastream_chunks)")
    p.add_argument("--marketing-threshold", type=float, default=0.8,
                   help="marketing_check 'Marketing Spam' probability to flag (default: 0.8)")
    p.add_argument("--max-posts-per-day", type=float, default=100.0,
                   help="lifetime posts/day at or above which an author looks automated "
                        "(default: 100)")
    p.add_argument("--follow-ratio", type=float, default=10.0,
                   help="follows-to-followers ratio flagged as follow-spam (default: 10)")
    p.add_argument("--min-followers", type=int, default=50,
                   help="follow-ratio heuristic only applies below this follower count "
                        "(default: 50)")
    p.add_argument("--spam-labels", default=DEFAULT_SPAM_LABELS)
    p.add_argument("--bot-labels", default=DEFAULT_BOT_LABELS)
    p.add_argument("--adult-labels", default=DEFAULT_ADULT_LABELS)
    args = p.parse_args()

    paths = list(args.files)
    if args.s3_latest:
        try:
            keys = s3_latest_keys(args.s3_latest, args.s3_stride)
        except subprocess.CalledProcessError as exc:
            print(f"S3 listing failed: {exc.stderr.strip()}", file=sys.stderr)
            return 1
        for key in keys:
            print(f"downloading {key} ...", file=sys.stderr)
            try:
                paths.append(s3_download(key, args.cache_dir))
            except subprocess.CalledProcessError as exc:
                print(f"  download failed: {exc.stderr.strip()}", file=sys.stderr)

    if not paths:
        print("no input files (pass paths or --s3-latest N)", file=sys.stderr)
        return 1

    stats = Stats(args)
    tmpdir = tempfile.mkdtemp(prefix="megastream_drop_")
    try:
        for path in paths:
            print(f"reading {os.path.basename(path)} ...", file=sys.stderr)
            try:
                stats.add_file(path, tmpdir)
            except (ValueError, sqlite3.DatabaseError) as exc:
                print(f"  skipped: {exc}", file=sys.stderr)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    print(f"\nanalyzed {len(paths)} chunk(s)")
    stats.report()
    return 0


if __name__ == "__main__":
    sys.exit(main())
