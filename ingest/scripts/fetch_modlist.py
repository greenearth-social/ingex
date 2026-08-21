#!/usr/bin/env python3
"""
Download an AT Protocol moderation list in one request.

`app.bsky.graph.getList` and `com.atproto.repo.listRecords` both page at 100
records. A 280k-member list is ~2,800 sequential round-trips, roughly three
hours. `com.atproto.sync.getRepo` returns the curator's entire repository as a
single CAR file instead — for that same list, 94 MB in about six seconds.

The repo holds every record the curator ever wrote, so this scans all blocks
and keeps `app.bsky.graph.listitem` records pointing at the requested list.
That is simpler and faster than walking the MST, and the result is identical
for our purposes: we want the membership set, not a proof it is complete.

Usage:
  # by list URL as shown in the Bluesky app
  python fetch_modlist.py https://bsky.app/profile/did:plc:.../lists/3lcm... -o list.txt

  # or by at:// URI
  python fetch_modlist.py at://did:plc:.../app.bsky.graph.list/3lcm... -o list.txt

  # keep the CAR around to re-extract other lists from the same curator
  python fetch_modlist.py <uri> -o list.txt --car-file curator.car

Output is one DID per line. Dependencies: stdlib only.
"""

import argparse
import json
import os
import re
import sys
import urllib.parse
import urllib.request

PLC_DIRECTORY = "https://plc.directory/"
LISTITEM_TYPE = "app.bsky.graph.listitem"


def parse_list_uri(value):
    """Return (repo_did, at_uri) from an at:// URI or a bsky.app list URL."""
    if value.startswith("at://"):
        parts = value[len("at://"):].split("/")
        if len(parts) != 3:
            raise ValueError(f"malformed at:// URI: {value}")
        return parts[0], value
    match = re.search(r"/profile/([^/]+)/lists/([^/?#]+)", value)
    if not match:
        raise ValueError(f"not a recognizable list URL or at:// URI: {value}")
    actor, rkey = match.group(1), match.group(2)
    if not actor.startswith("did:"):
        url = ("https://public.api.bsky.app/xrpc/app.bsky.actor.getProfile?actor="
               + urllib.parse.quote(actor))
        with urllib.request.urlopen(url, timeout=30) as resp:
            actor = json.load(resp)["did"]
    return actor, f"at://{actor}/app.bsky.graph.list/{rkey}"


def resolve_pds(did):
    with urllib.request.urlopen(PLC_DIRECTORY + did, timeout=30) as resp:
        doc = json.load(resp)
    for service in doc.get("service") or []:
        if service.get("id", "").endswith("atproto_pds"):
            return service["serviceEndpoint"].rstrip("/")
    raise ValueError(f"{did} declares no #atproto_pds service")


# ── minimal CARv1 + DAG-CBOR reader ──────────────────────────────────────
#
# Only enough of each format to walk blocks and decode records. DAG-CBOR is
# CBOR restricted to deterministic encoding, plus tag 42 for CIDs.


class Reader:
    def __init__(self, buf):
        self.buf = buf
        self.pos = 0

    def eof(self):
        return self.pos >= len(self.buf)

    def take(self, n):
        chunk = self.buf[self.pos:self.pos + n]
        if len(chunk) != n:
            raise EOFError("truncated CAR")
        self.pos += n
        return chunk

    def varint(self):
        result = shift = 0
        while True:
            byte = self.take(1)[0]
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:
                return result
            shift += 7

    def cid(self):
        """Consume a CID and return nothing useful — we only need to skip it."""
        start = self.pos
        first = self.take(1)[0]
        if first == 0x12:               # CIDv0: sha2-256, 32 bytes
            self.take(1)
            self.take(32)
            return self.buf[start:self.pos]
        self.varint()                   # codec (version byte already consumed)
        self.varint()                   # multihash code
        length = self.varint()
        self.take(length)
        return self.buf[start:self.pos]


def decode_cbor(r):
    initial = r.take(1)[0]
    major, extra = initial >> 5, initial & 0x1F

    if extra < 24:
        value = extra
    elif extra == 24:
        value = r.take(1)[0]
    elif extra == 25:
        value = int.from_bytes(r.take(2), "big")
    elif extra == 26:
        value = int.from_bytes(r.take(4), "big")
    elif extra == 27:
        value = int.from_bytes(r.take(8), "big")
    elif extra == 31:
        value = None                    # indefinite length; not valid DAG-CBOR
    else:
        raise ValueError(f"bad CBOR additional info {extra}")

    if major == 0:
        return value
    if major == 1:
        return -1 - value
    if major == 2:
        return r.take(value)
    if major == 3:
        return r.take(value).decode("utf-8", "replace")
    if major == 4:
        return [decode_cbor(r) for _ in range(value)]
    if major == 5:
        out = {}
        for _ in range(value):
            key = decode_cbor(r)
            out[key] = decode_cbor(r)
        return out
    if major == 6:
        inner = decode_cbor(r)          # tag 42 wraps a CID byte string
        return inner
    if major == 7:
        if extra == 20:
            return False
        if extra == 21:
            return True
        if extra in (22, 23):
            return None
        return value
    raise ValueError(f"unhandled CBOR major type {major}")


def iter_car_records(data):
    """Yield every decodable DAG-CBOR block in a CARv1 payload."""
    r = Reader(data)
    header_len = r.varint()
    r.take(header_len)
    while not r.eof():
        try:
            block_len = r.varint()
        except EOFError:
            return
        end = r.pos + block_len
        try:
            r.cid()
            block = Reader(r.buf[r.pos:end])
            yield decode_cbor(block)
        except (EOFError, ValueError, UnicodeDecodeError, IndexError):
            pass
        r.pos = end


def main():
    p = argparse.ArgumentParser(
        description="Download an AT Protocol moderation list via getRepo"
    )
    p.add_argument("list_uri", help="at:// URI or bsky.app list URL")
    p.add_argument("-o", "--output", required=True, help="write DIDs here, one per line")
    p.add_argument("--car-file",
                   help="path to keep (or reuse) the downloaded CAR")
    args = p.parse_args()

    repo_did, list_uri = parse_list_uri(args.list_uri)
    print(f"list  {list_uri}", file=sys.stderr)

    car_path = args.car_file
    if car_path and os.path.exists(car_path):
        print(f"reusing {car_path}", file=sys.stderr)
        with open(car_path, "rb") as fh:
            data = fh.read()
    else:
        pds = resolve_pds(repo_did)
        url = f"{pds}/xrpc/com.atproto.sync.getRepo?did={urllib.parse.quote(repo_did)}"
        print(f"fetching repo from {pds} ...", file=sys.stderr)
        with urllib.request.urlopen(url, timeout=600) as resp:
            data = resp.read()
        print(f"  {len(data) / 1e6:.1f} MB", file=sys.stderr)
        if car_path:
            with open(car_path, "wb") as fh:
                fh.write(data)

    dids, other_lists = [], 0
    seen = set()
    for record in iter_car_records(data):
        if not isinstance(record, dict) or record.get("$type") != LISTITEM_TYPE:
            continue
        if record.get("list") != list_uri:
            other_lists += 1
            continue
        subject = record.get("subject")
        if isinstance(subject, str) and subject not in seen:
            seen.add(subject)
            dids.append(subject)

    with open(args.output, "w") as fh:
        for did in dids:
            fh.write(did + "\n")
    print(f"wrote {len(dids):,} DIDs to {args.output}", file=sys.stderr)
    if other_lists:
        print(f"  ({other_lists:,} listitems belonged to the curator's other lists)",
              file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
