#!/usr/bin/env python3
"""
What moderation labels appear in megastream data, who issues them, and where?

Companion to megastream_drop_analysis.py. That script asks how much we could
drop; this one asks what vocabulary is available to drop things *by*.

Labels reach us through four distinct carriers, and the difference matters:

  1. account labels  - raw_post.hydrated_metadata.user.labels
     Labeler-applied, attached to the post's author. Present at ingest time
     because the account already existed. This is the only carrier that is
     both actionable at ingest and issued by a third party.

  2. post self-labels - raw_post.message.commit.record.labels.values[]
     Applied by the author to their own post, in the record itself. Always
     present, never independent — a spammer will not self-label as spam. In
     practice these are content warnings (porn, nudity, graphic-media).

  3. referenced-post labels - hydrated_metadata.{reply,parent,quote}_post.labels
     Labeler-applied to an *older* post that this one replies to or quotes.

  4. referenced-post author labels - ...{reply,parent,quote}_post.author.labels

Carrier 3 is the interesting absence: there is no labeler label on the post
being ingested, because the post is seconds old when we capture it and no
labeler has seen it yet. Post-level moderation is therefore not available at
ingest time no matter how we restructure. Account-level is.

Usage:
  python megastream_label_inventory.py chunks/*.db.zip
  python megastream_label_inventory.py chunks/*.db.zip --resolve-handles

--resolve-handles looks each labeler DID up against the public Bluesky
appview (public.api.bsky.app, unauthenticated) to turn did:plc:... into a
readable handle. It is off by default so the script stays offline-capable.

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
import urllib.error
import urllib.request
import zipfile

PUBLIC_APPVIEW = "https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile?actor="

# Carriers, in the order the report presents them.
#
# Account labels split by issuer: a label whose `src` is the labelled account's
# own DID was applied by that account to itself (this is how
# !no-unauthenticated and account-level adult flags arrive), and carries no
# independent signal. Only third-party labels represent someone else's verdict.
ACCOUNT = "account labels, THIRD-PARTY (another labeler's verdict)"
ACCOUNT_SELF = "account labels, SELF-APPLIED (the account's own setting)"
SELF = "post self-labels (author-applied, this post)"
REF_POST = "referenced-post labels (labeler-applied, older post)"
REF_AUTHOR = "referenced-post author labels"


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


def iter_labels(node):
    """Yield (val, src) from a labels field, tolerating both shapes.

    Labeler output is a list of label objects; self-labels are a
    `com.atproto.label.defs#selfLabels` object wrapping a `values` list whose
    entries carry only `val`. Negated and expired labels are skipped.
    """
    if isinstance(node, dict):
        node = node.get("values") or []
    if not isinstance(node, list):
        return
    for label in node:
        if not isinstance(label, dict):
            continue
        if label.get("neg"):
            continue
        val = label.get("val")
        if val:
            yield val, label.get("src")


class Inventory:
    def __init__(self):
        self.chunks = 0
        self.creates = 0
        # carrier -> Counter of val
        self.by_carrier = collections.defaultdict(collections.Counter)
        # carrier -> val -> set of issuer DIDs
        self.issuers = collections.defaultdict(lambda: collections.defaultdict(set))
        # val -> set of labelled account DIDs (account carrier only)
        self.accounts_by_val = collections.defaultdict(set)
        self.src_totals = collections.Counter()
        self.posts_with_account_label = 0

    def add_file(self, path, tmpdir):
        db = sniff_open_sqlite(path, tmpdir)
        con = sqlite3.connect(db)
        for did, raw_post in con.execute("SELECT did, raw_post FROM enriched_posts"):
            self.add_row(did, raw_post)
        con.close()
        self.chunks += 1

    def record(self, carrier, val, src, account_did=None):
        self.by_carrier[carrier][val] += 1
        if src:
            self.issuers[carrier][val].add(src)
            self.src_totals[src] += 1
        if account_did:
            self.accounts_by_val[val].add(account_did)

    def add_row(self, did, raw_post):
        try:
            post = json.loads(raw_post)
        except (TypeError, ValueError):
            return
        commit = (post.get("message") or {}).get("commit") or {}
        if commit.get("operation") != "create":
            return
        self.creates += 1

        record = commit.get("record") or {}
        for val, src in iter_labels(record.get("labels")):
            self.record(SELF, val, src)

        hydrated = post.get("hydrated_metadata") or {}
        user = hydrated.get("user") or {}
        subject_did = user.get("did") or did
        had_third_party = False
        for val, src in iter_labels(user.get("labels")):
            if src and src == subject_did:
                self.record(ACCOUNT_SELF, val, src, account_did=did)
            else:
                self.record(ACCOUNT, val, src, account_did=did)
                had_third_party = True
        if had_third_party:
            self.posts_with_account_label += 1

        for key in ("reply_post", "parent_post", "quote_post"):
            node = hydrated.get(key)
            if not isinstance(node, dict):
                continue
            for val, src in iter_labels(node.get("labels")):
                self.record(REF_POST, val, src)
            author = node.get("author")
            if isinstance(author, dict):
                for val, src in iter_labels(author.get("labels")):
                    self.record(REF_AUTHOR, val, src)

    def report(self, handles):
        print(f"\nanalyzed {self.chunks} chunk(s), {self.creates:,} post creates")
        print(f"posts whose author carries a third-party label: "
              f"{self.posts_with_account_label:,} "
              f"({100.0 * self.posts_with_account_label / max(self.creates, 1):.1f}%)")

        for carrier in (ACCOUNT, ACCOUNT_SELF, SELF, REF_POST, REF_AUTHOR):
            counts = self.by_carrier.get(carrier)
            print(f"\n{'=' * 74}\n{carrier}\n{'=' * 74}")
            if not counts:
                print("  (none observed)")
                continue
            width = max(len(v) for v in counts)
            for val, count in counts.most_common(40):
                srcs = self.issuers[carrier].get(val, set())
                if carrier in (ACCOUNT, ACCOUNT_SELF):
                    extra = f"{len(self.accounts_by_val[val]):>6,} accts"
                else:
                    extra = " " * 11
                issuer = ""
                if srcs:
                    names = sorted(handles.get(s, s) for s in srcs)
                    issuer = f"  from {names[0]}" + (
                        f" +{len(names) - 1} more" if len(names) > 1 else ""
                    )
                print(f"  {count:>7,} posts  {extra}  {val:<{width}}{issuer}")

        print(f"\n{'=' * 74}\nlabel issuers by volume (includes self-applied)\n{'=' * 74}")
        for src, count in self.src_totals.most_common(15):
            print(f"  {count:>7,}  {handles.get(src, src)}")
        if not self.src_totals:
            print("  (none observed)")


def resolve_handles(dids):
    """Map labeler DIDs to handles via the public appview. Best-effort."""
    handles = {}
    for did in dids:
        try:
            with urllib.request.urlopen(PUBLIC_APPVIEW + did, timeout=10) as resp:
                profile = json.load(resp)
            handle = profile.get("handle")
            if handle:
                handles[did] = f"{handle}  ({did})"
        except (urllib.error.URLError, ValueError, TimeoutError) as exc:
            print(f"  could not resolve {did}: {exc}", file=sys.stderr)
    return handles


def main():
    p = argparse.ArgumentParser(
        description="Inventory the moderation labels present in megastream chunks"
    )
    p.add_argument("files", nargs="+", help="megastream .db / .db.zip chunks")
    p.add_argument("--resolve-handles", action="store_true",
                   help="look labeler DIDs up against the public Bluesky appview")
    p.add_argument("--resolve-limit", type=int, default=30,
                   help="how many of the busiest issuers to resolve (default: 30)")
    args = p.parse_args()

    inv = Inventory()
    tmpdir = tempfile.mkdtemp(prefix="megastream_labels_")
    try:
        for path in args.files:
            print(f"reading {os.path.basename(path)} ...", file=sys.stderr)
            try:
                inv.add_file(path, tmpdir)
            except (ValueError, sqlite3.DatabaseError) as exc:
                print(f"  skipped: {exc}", file=sys.stderr)
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

    handles = {}
    if args.resolve_handles:
        # Self-applied labels make every labelled account its own "issuer", so
        # the tail runs to thousands of DIDs. Only the head is worth a lookup.
        top = [src for src, _ in inv.src_totals.most_common(args.resolve_limit)]
        print(f"resolving {len(top)} labeler handles ...", file=sys.stderr)
        handles = resolve_handles(top)

    inv.report(handles)
    return 0


if __name__ == "__main__":
    sys.exit(main())
