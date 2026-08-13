# Filtering inauthentic accounts out of ingest

Investigation, 2026-08-12/13. Target: accounts no human would willingly read or
follow — platform manipulation, engagement farming, bot farms. Both a data
quality concern (bot farms are the standard vehicle for political
manipulation) and a cost one (kNN indices are RAM-hungry).

Not in scope: self-declared bots and adult content. Both have willing
audiences.

Sample: 16 megastream chunks, 63,341 post creates from 44,856 distinct
authors, strided across ~3 days ending 2026-08-12.

## Conclusions

1. **Subscribe to Skywatch Blue, whitelisting its abuse labels.** It flags
   **6.52% of ingested posts across 1,367 authors**, against 0.2% for
   Bluesky's own labeler. Nothing else came close.
2. **Apply it to likes, not just posts.** Labels attach to accounts, so the
   same DID set works on both — and the like path is where bot-farm damage
   actually lands.
3. **Mark at ingest, filter at retrieval.** Dropping is irreversible; a
   labeler false positive would silently erase an account forever.
4. **Don't build this in-house.** Follower count, the obvious proxy, does not
   separate these accounts at all.

## Why Skywatch

Bluesky's own `moderation.bsky.app` is the only labeler whose labels reach us
in the megastream payload, and it finds almost nothing in live data: 0.2% of
posts, 32 accounts out of 44,856. That is consistent with Bluesky enforcing
against what it detects, so the accounts it has judged are mostly gone before
they reach us.

Skywatch, restricted to its abuse vocabulary:

| Label | Posts | Accounts |
| --- | --- | --- |
| platform-manipulation | 2.07% | 285 |
| suspect-inauthentic | 1.88% | 314 |
| spam | 1.85% | 237 |
| repetitive-domain-spam-sustained | 1.75% | 214 |
| repetitive-domain-spam-burst | 1.40% | 161 |
| follow-farming | 0.66% | 262 |
| bulk-following | 0.63% | 213 |
| amplifier | 0.62% | 109 |
| engagement-abuse | 0.30% | 86 |
| inauthentic-fundraising | 0.14% | 33 |
| **union** | **6.52%** | **1,367** |

For political manipulation specifically the on-target values are
`platform-manipulation`, `suspect-inauthentic`, `amplifier`, and
`disinformation-network`.

**Whitelist label values; never subscribe wholesale.** Skywatch ships its
abuse vocabulary in the same subscription as `maga-trump`, `tankie`,
`hammer-sickle`, `terf-gc`, `inverted-red-triangle`, and `elon-musk`. Taking
the labeler as a unit imports viewpoint filtering nobody agreed to. It also
emits `bluesky-elder` on 15.8% of posts, which is pure noise here.

## Why not a static list

Casey Ho's modlist ("Platform Manipulation, Spam, & Coordinated Inauthentic
Behavior") holds 282,859 accounts and matches **41 of our 44,856 authors —
0.25% of posts.**

The reason is that a static list ages into a graveyard: **91.7% of a random
120-account sample from it no longer resolves at all** — deleted, deactivated,
or taken down. It records accounts the platform has already removed, and
removed accounts do not post. Where it does hit a live author, Skywatch agrees
56% of the time, so it is late rather than wrong.

Retrieval cost is not the differentiator. `scripts/fetch_modlist.py` pulls an
entire 282k-member list via `com.atproto.sync.getRepo` as one 94 MB CAR in
about six seconds, versus ~2,800 paged requests. Freshness is the problem with
lists, not bandwidth.

## Why not in-house heuristics

**Follower count does not work.** Accounts Skywatch flags for abuse have a
**median 442 followers against 427 for everyone else** — indistinguishable,
because follow-farming and reciprocal-follow schemes give them ordinary-looking
social graphs. A `followers < 100` rule catches 25% of flagged accounts while
sweeping in 10,936 unflagged ones.

Post rate separates better (median 25.7/day flagged vs 4.7/day) but still
catches prolific humans and news bots.

The labeler supplies information we cannot cheaply reconstruct. That is the
core argument for adopting one.

## Applying this to likes

**The like path is the higher-stakes surface.** `posts_recent_quality`
requires `like_count >= 20`. That bar screens out human posts that landed
badly; it does nothing to a bot farm, which manufactures its own likes.
Inauthentic engagement clears the threshold by construction, so coordinated
accounts are *promoted* into the lean retrieval corpus and into two-tower
training rather than filtered out. Engagement thresholds are the wrong
instrument against manipulation, because manipulation is a supply of
engagement.

**No post is needed to make the decision.** Account-level labels come back
from `queryLabels` with `uri: "did:plc:..."`, and a like record carries its
author's DID. One maintained DID set filters likes and posts alike. This also
sidesteps a structural limit: labeler labels are never on a freshly-created
post — the post is seconds old and no labeler has seen it — but an account
label predates the like.

**`jetstream_ingest` is the natural home.** It already consumes a websocket
with cursor state, and already exports a DID blocklist via
`GE_BLOCKLIST_DESTINATION`.

### Keeping the DID set current

Two confirmed transports on `ozone.skywatch.blue`:

- **`queryLabels?uriPatterns=*`** — plain HTTP, integer cursor, ~595
  labels/sec. What `scripts/enumerate_labeler.py` uses. Full run: **1,133,558
  labels in 32 minutes → 128,587 account DIDs** across 79 values, 111,602 of
  them under the abuse vocabulary. Incremental refresh from a stored cursor
  takes seconds.
- **`subscribeLabels?cursor=0`** — websocket; replays full history then tails
  live. Confirmed working, **but only over HTTP/1.1** — over HTTP/2 the
  endpoint 404s. The better long-run basis, since its cursor is a sequence
  number by specification.

Largest account-level label sets from the full enumeration:

| Label | Accounts |
| --- | --- |
| bulk-following | 67,874 |
| platform-manipulation | 18,326 |
| suspect-inauthentic | 13,209 |
| inauthentic-fundraising | 9,428 |
| engagement-abuse | 8,883 |
| spam | 7,729 |
| follow-farming | 1,362 |

**Honour retractions.** They arrive as records with `neg: true`. Without
applying them the exclusion set only grows and never forgives — which is what
makes occasional false positives tolerable.

**Enumerate rather than batch-query.** Per-DID lookups across 45k authors got
us 403-ed by `aimod.social` partway through and then blocked entirely. One
ordered scan is faster and far gentler on the operator.

Note that Skywatch's labels are majority **post-level** — in a 5,000-label
sample, 3,199 `at://` post URIs against 1,801 account labels. Only account
labels are usable for likes; `enumerate_labeler.py` keeps those by default.

## Open questions

- **Enumeration looks incomplete.** Scoring our authors against the enumerated
  set gives 5.96% of posts where the targeted per-DID sweep found 6.52% — 83%
  recovery. Two from-scratch wildcard scans also ended on inconsistent
  cursors, suggesting `uriPatterns=*` ordering is not stable and a cursor walk
  can skip records. Reconcile against a targeted sweep, or use
  `subscribeLabels`, before treating enumeration as the source of truth.
- **What share of ingested *likes* comes from flagged accounts?** Unmeasured;
  this investigation covered posts only. Expect it to be higher, since farming
  likes is cheaper than farming posts. This is the number that would justify
  the work.
- **Does Skywatch's coverage hold up over time?** One 3-day sample. Re-run
  before committing.

## Labelers evaluated and set aside

| Labeler | Verdict |
| --- | --- |
| `labeler.hailey.at` | **Dead.** Best vocabulary after Skywatch (`coordinated-abuse`, `ai-agent`, `mass-follow-high`), but its host serves an unrelated web app ("G1XL V2") on every path including `_health`, behind a cert for `qeezi.com`, while its DID document still advertises it. |
| `aimod.social` | **Unmeasured.** AI-imagery labels (`ai-imagery`, `user-frequent-ai-imagery`) are in scope, but it rate-limits bulk access hard and blocked us mid-sweep. Retry gently, via enumeration. |
| `perisai.bsky.social` | Indonesian-language and regionally scoped. `olgambling`, `scam`, `affiliator`, `crypto`, `hoax` are on-target; our corpus is 73% English, so expect thin coverage. (`autobase` is a legitimate anonymous-relay format, not spam.) |
| `profile-labels.bossett.social` | Skipped. `rapidposts` / `onlyreplies` are behavioural descriptions, not abuse verdicts. |

## Signals in the megastream payload

None of these reach Elasticsearch today — `megastream_message.go` reads about
fifteen fields per row and discards the rest.

**Account labels** (`hydrated_metadata.user.labels`) carry only labels from
labelers the *hydrating client* subscribes to, in practice just
`moderation.bsky.app`. Third-party labelers must be queried directly; that is
why this work needed its own tooling.

**Read the issuer.** A label whose `src` equals the account's own DID is
self-applied and carries no independent verdict. This is how
`!no-unauthenticated` (22% of posts — a privacy preference), `bot`, and the
`bridged-from-*` markers arrive. Reading them as moderation verdicts overstates
any answer by an order of magnitude; an earlier pass here reported "2%
labelled bot/spam" that was almost entirely honest self-declaration.

**`marketing_check`** (`inferences.text.<path>.marketing_check`) is upstream's
per-post spam classifier, on 97% of posts, and does not work. Scores bunch
mid-range (median 0.42), so the flagged share swings from 33.6% at ≥0.5 to
1.1% at ≥0.8 — the threshold decides the answer, not the data. It does not
discriminate: posts matching obvious spam patterns scored a median 0.49
against 0.42 for everything else, and the 0.8+ tail is ordinary political
chatter. Do not use without recalibration against labelled examples.

**Post self-labels** (`record.labels.values[]`) are author-applied content
warnings, and partly abused — Bridgy Fed stuffs metadata through the field as
pseudo-labels (`reaction_count:104`). Any consumer needs a whitelist.

## Reproducing

```bash
set -a && . ingest/.env && set +a          # AWS credentials

# sample chunks spanning ~3 days
python scripts/megastream_drop_analysis.py --s3-latest 12 --s3-stride 200 \
    --cache-dir ./chunks

# what a labeler would catch, weighted by post volume
python scripts/megastream_labeler_coverage.py chunks/*.db.zip \
    --labeler skywatch.blue --cache-file skywatch.json

# build and refresh the DID set
python scripts/enumerate_labeler.py --labeler skywatch.blue -o skywatch_dids.json
python scripts/enumerate_labeler.py --labeler skywatch.blue -o skywatch_dids.json \
    --resume-from skywatch_dids.json

# a moderation list, in one request
python scripts/fetch_modlist.py <bsky.app list URL> -o list.txt
```

Practical notes for anyone reading chunks directly:

- The bucket (`graze-mega-02`, prefix `mega/`) is **requester-pays**: `aws`
  calls need `--request-payer requester`, and we are billed for transfer.
- Volume is ~800 chunks/day at ~115 MB, about 90 GB/day. Sample with
  `--s3-stride` rather than downloading a contiguous span.
- Current chunks are raw SQLite despite the `.db.zip` suffix, in an
  `enriched_posts` table. The spooler sniffs for this; ad-hoc tooling must too.
- Chunks overlap. Shared `at_uri`s between adjacent chunks are create-in-one /
  delete-in-the-next; all scripts here deduplicate.
