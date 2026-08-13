# How much megastream data could we skip indexing?

Investigation, 2026-08-12. Question: accounts tagged as spam or bots can be
ignored in our indices — what share of what we ingest is that, and what
attributes would we need to identify it?

Sample: 16 megastream chunks, ~63,300 post creates from ~44,900 distinct
authors, strided across roughly three days ending 2026-08-12. Reproduce with
`scripts/megastream_drop_analysis.py` and `scripts/megastream_label_inventory.py`.

## Answer

**Labeler-confirmed spam is negligible: 0.2% of posts, 32 accounts out of
44,856.** Self-declared bots are an order of magnitude more common at 2.9%.
Everything label-based together — spam verdicts, self-declared bots, and adult
labels — comes to 4.3%.

| Category | Posts | Accounts |
| --- | --- | --- |
| Self-declared bot account | 2.9% | 628 |
| Labeler spam verdict | 0.2% | 32 |
| Account labelled adult | 1.2% | — |
| **Union, label-based** | **4.3%** | — |
| Heuristic: author posts ≥100/day | 9.0% | — |
| Union including heuristics | 13.0% | — |

Two much larger slices turned up along the way: **replies are 45.5%** of
ingested rows (they go to the separate `replies` index and never enter the kNN
corpus), and **26.9% of posts declare a language other than `en`**. Either
dwarfs every spam category combined.

## The signals, and which ones can be trusted

Nothing below currently reaches Elasticsearch. `megastream_message.go` reads
about fifteen fields per row and discards the rest.

### Account labels — usable, but read the issuer

`raw_post.hydrated_metadata.user.labels` carries the author's labels, present
at ingest time because the account already existed. The critical distinction
is `src`: when it equals the account's own DID the label is **self-applied**
and carries no independent verdict.

Getting this wrong inflates the answer badly. An earlier pass counted `bot` as
a moderation verdict and reported "2% labelled bot/spam"; almost all of it was
accounts honestly declaring their own automation.

Third-party verdicts come from essentially one labeler, `moderation.bsky.app`
(4,006 labels in the sample; the next busiest issued 639, and the long tail is
individual users self-labelling). Its full observed vocabulary:

```
porn  spam  sexual  nudity  rude  sexual-figurative  graphic-media
intolerant  !unspecced-takedown  self-harm  impersonation  !hide
```

Self-applied labels are dominated by `!no-unauthenticated` (22% of posts —
a **privacy preference**, not a moderation signal, and the single easiest way
to overstate this analysis by an order of magnitude), then `bot` (1,811
posts / 626 accounts), then the Bridgy Fed `bridged-from-*` markers.

`bot` is worth taking seriously despite being self-declared: it is the AT
Protocol convention for honest automation, it covers 628 accounts, and those
accounts are openly not human. It will never catch a spammer pretending to be
human, so it is a content-type filter rather than an abuse filter.

### Post self-labels — narrow, and partly abused

`raw_post.message.commit.record.labels.values[]`, author-applied to their own
post: `porn` (319), `sexual`, `nudity`, `graphic-media`. Also carries junk —
Bridgy Fed stuffs non-moderation metadata through this field as pseudo-labels
(`reaction_count:104`, `comment_count:27`, `child_comment_count:22`). Anything
consuming self-labels needs a whitelist, not a passthrough.

### Post-level moderation — structurally unavailable

There is no labeler label on the post being ingested, and there cannot be: the
post is seconds old when we capture it and no labeler has seen it. Labeler
labels on posts do appear in the data, but only on *older* posts referenced as
reply parents or quotes (`hydrated_metadata.{reply,parent,quote}_post.labels`).

This is a hard constraint on any design: **account-level moderation is
available at ingest, post-level is not.** Post-level filtering would require a
backfill pass or a labeler subscription, not a change to how we read chunks.

### `marketing_check` — do not use as-is

`inferences.text.<field-path>.marketing_check` is upstream's per-post spam
classifier, present on 97% of posts, and on this evidence it does not work.

- Scores bunch mid-range: median 0.42, p99 0.82. The flagged share swings from
  33.6% at ≥0.5 to 1.1% at ≥0.8, so the threshold, not the data, decides the
  answer.
- It does not discriminate. Posts matching obvious spam patterns (line.me and
  Telegram invites, promo codes, crypto) scored a median 0.49 against 0.42 for
  everything else.
- Hand-reading the 0.8+ tail turned up ordinary political and conversational
  posts. An unmistakable chat-room recruitment spam carrying a `line.me` link
  scored 0.49.

It stays in the report because it is the only per-post spam signal upstream
gives us and may just need recalibration, but nothing should be dropped on it
until it is validated against labelled examples.

### Heuristics — a rate is not a verdict

Author `posts_count` / account age flags 9.0% of posts at ≥100/day, making it
the largest single contributor to the 13% figure and the least trustworthy:
prolific humans and legitimate news bots both clear the bar. Treat it as an
upper bound on a population worth investigating, not a droppable set.

## Practical notes

- The bucket (`graze-mega-02`, prefix `mega/`) is **requester-pays**: `aws`
  calls need `--request-payer requester` and we are billed for transfer.
- Volume is ~800 chunks/day at ~115 MB, about 90 GB/day. Sample with
  `--s3-stride` rather than downloading a contiguous span.
- Chunks genuinely overlap. Shared `at_uri`s across adjacent chunks are
  create-in-one / delete-in-the-next; both analysis scripts deduplicate.
- Current chunks are raw SQLite despite the `.db.zip` suffix. The spooler
  sniffs for this; ad-hoc tooling must too.

## Third-party labelers (follow-up, same sample)

Bluesky's own labeler is not the only source, and it is by far the weakest for
this purpose. `scripts/megastream_labeler_coverage.py` asks a labeler's Ozone
instance about our ingested authors directly, via
`com.atproto.label.queryLabels`, and weights the answer by post volume.

This has to be a direct query. The `hydrated_metadata.user.labels` in the
payload only carries labels from labelers the *hydrating client* subscribes
to — in practice just `moderation.bsky.app`. Third-party labels are invisible
to us until we ask for them.

**Skywatch Blue (`skywatch.blue`) catches 33x more than Bluesky's labeler.**
Restricted to its abuse vocabulary — dropping its political and informational
labels — it flags **6.52% of ingested posts across 1,367 accounts**, against
0.2% for `moderation.bsky.app`:

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
| **union, abuse labels only** | **6.52%** | **1,367** |

Skywatch also emits political and ideological labels — `maga-trump`,
`tankie`, `hammer-sickle`, `terf-gc`, `inverted-red-triangle`, `elon-musk`,
`fringe-media`, `intolerance` — plus informational ones like `bluesky-elder`
(15.8% of posts, and meaningless here). Subscribing wholesale would import
viewpoint filtering we have not agreed to. Any adoption must whitelist
specific label values, which is what `--ignore-labels` exists to model.

Declared vocabularies of the other candidates, via
`app.bsky.labeler.getServices?detailed=true`:

- **`labeler.hailey.at`** — **dead, not worth pursuing.** Its declared
  vocabulary was the best complement to Skywatch (`coordinated-abuse`,
  `ai-agent`, `general-spam`, `mass-follow-high`, `reply-link-spam`), but the
  endpoint is gone. `ozone.hailey.at` serves a certificate for `CN=qeezi.com`,
  and with verification disabled it returns HTTP 200 with the HTML of an
  unrelated web app ("G1XL V2") on every path including `_health`. The DID
  document still advertises it as the labeler service; nothing is there.
- **`aimod.social`** — `ai-imagery`, `ai-avatar-or-banner`,
  `user-frequent-ai-imagery`, relevant if AI-generated slop is in scope.
  Serves single queries fine, but **rate-limits bulk access hard**: a per-DID
  sweep started returning 403s a few thousand authors in, and afterwards the
  wildcard enumeration was refused outright. Coverage is unmeasured. Retry
  later, gently, via enumeration rather than per-DID batches.
- **`perisai.bsky.social`** — Indonesian-language, and regionally scoped: its
  profile translates as "an account labelling system to protect you from
  seeing accounts and content you do or do not want in your timeline."
  Label values, translated:

  | Label | Meaning |
  | --- | --- |
  | `autobase` | relays messages from users anonymously (a popular Indonesian format) |
  | `roleplayer` | roleplay account |
  | `olgambling` | offers online gambling |
  | `scam` | fraud, impersonating some entity |
  | `impersonation` | imitates another figure, for jokes or fraud |
  | `affiliator` | affiliate-marketing account |
  | `crypto` | promotes cryptocurrency |
  | `hoax` | false information intended to mislead about a fact |
  | `hatespeech` / `rudecomment` | hate speech / abusive language |
  | `ncseventeen`, `pornography`, `adultery`, `prostitution` | adult categories |

  The commercially-motivated ones (`olgambling`, `scam`, `affiliator`,
  `crypto`, `hoax`) are on-target, but the labeler is aimed at Indonesian
  Bluesky and our corpus is 73% English, so expect small coverage. Cheap to
  measure if we want the number.
- **`profile-labels.bossett.social`** — skipped by decision: `rapidposts` /
  `onlyreplies` are behavioural descriptions, not abuse verdicts.

For political manipulation specifically, the on-target label values across all
of these are Skywatch's `platform-manipulation`, `disinformation-network`,
`amplifier`, and `suspect-inauthentic`, plus Hailey's `coordinated-abuse`.

### Follower count is not a substitute

The obvious in-house proxy does not work. Accounts Skywatch flags for abuse
have a **median 442 followers against 427 for everyone else** — statistically
indistinguishable, because follow-farming and reciprocal-follow schemes give
these accounts ordinary-looking social graphs. A `followers < 100` rule
catches only 25% of flagged accounts while sweeping in 10,936 unflagged ones.

Post rate separates better (median 25.7/day flagged vs 4.7/day) but still
catches prolific humans and news bots.

So the labeler supplies information we cannot cheaply reconstruct. That is the
strongest argument for adopting one.

## If we act on this

**Scope.** The target is accounts no human would willingly read: platform
manipulation, engagement farming, spam. Explicitly *not* in scope —
self-declared bots and adult content both have willing audiences, so the 2.9%
and 1.2% lines above are measurements, not proposals. That leaves Skywatch's
6.5% as the number worth acting on.

**Prefer a labeler subscription to a static list — measured, not assumed.**
Casey Ho's modlist ("Platform Manipulation, Spam, & Coordinated Inauthentic
Behavior") holds 282,859 accounts, and matches **41 of our 44,856 ingested
authors — 0.25% of posts**, against Skywatch's 6.52%.

The reason is that a static list ages into a graveyard: **91.7% of a random
120-account sample from it no longer resolves at all** — deleted, deactivated,
or taken down. It is a historical record of accounts the platform has already
removed, and removed accounts do not post. Where it does hit a still-active
author, Skywatch agrees 56% of the time, so it is not wrong, just late.

A labeler tracks accounts while they are still posting, exposes `queryLabels`
for bulk lookup and `subscribeLabels` for a live feed, versions each label with
`cts`, and can *retract* one via `neg: true`. For something that decides what
enters our corpus, currency and retractability are what matter.

(Retrieval cost is not the differentiator: see `scripts/fetch_modlist.py`. A
whole 282k-member list comes down via `com.atproto.sync.getRepo` as one 94 MB
CAR in about six seconds, versus ~2,800 paged requests. Freshness is the
problem with lists, not bandwidth.)

**Do not drop at ingest — mark at ingest, filter at retrieval.** Dropping is
irreversible: we cannot re-derive a post we never indexed, so a labeler false
positive silently and permanently removes an account. Writing an
`author_spam_labels` keyword field costs one field per document and keeps
every decision reversible and auditable.

**Whitelist label values, never a whole labeler.** Skywatch's political
vocabulary is entangled with its abuse vocabulary in one subscription.

**The quality corpus does not handle this — it may amplify it.**
`posts_recent_quality` requires `like_count >= 20`. That bar screens out
*human* posts that landed badly; it does nothing to a bot farm, which
manufactures its own likes. Inauthentic engagement clears the threshold by
construction, which means coordinated accounts are *promoted* into the lean
retrieval corpus rather than filtered out of it, and their posts become
two-tower training examples. Engagement thresholds are the wrong instrument
against manipulation, because manipulation is a supply of engagement.

This makes the like path the higher-stakes surface, not the post path. Likes
feed user history for both training and serving, and they decide quality-corpus
membership.

### Applying labels to likes

**Labels attach to accounts, not posts, so no post is needed.** Account-level
labels come back from `queryLabels` with `uri: "did:plc:..."`, and a like
record carries its author's DID. The same maintained DID set filters likes and
posts alike. This also sidesteps the structural problem noted above — that
labeler labels are never on a freshly-created post — because an account label
predates the like.

Two confirmed transports for keeping that set current, both on
`ozone.skywatch.blue`:

- **`queryLabels?uriPatterns=*`** — plain HTTP, paginated by an integer
  cursor, ~610 labels/sec sustained. Resumable: saving the cursor and passing
  it back returns only labels newer than that point (verified — resuming from
  a stored cursor returned labels timestamped months after the ones a
  from-scratch scan starts with). This is what `scripts/enumerate_labeler.py`
  uses: full backfill once, then cheap incremental refreshes.
- **`subscribeLabels?cursor=0`** — websocket, replays the labeler's entire
  history then tails live. Confirmed working (`101 Switching Protocols`), but
  **only over HTTP/1.1** — the same request over HTTP/2 returns 404. This is
  the better long-run home, because `jetstream_ingest` is already a
  cursor-managing websocket consumer and a second subscription fits that shape
  exactly.

Retractions arrive as records with `neg: true` and must be applied, or the
exclusion set only ever grows and never forgives. `enumerate_labeler.py`
handles this by discarding the DID from that label's set.

Bulk enumeration is also simply the better-behaved approach: per-DID batch
lookups across ~45k authors got us 403-ed by `aimod.social` partway through,
while enumerating a labeler wholesale is one ordered scan and far gentler on
the operator.

Note that Skywatch's labels are majority **post-level** — in a 5,000-label
sample, 3,199 were `at://` post URIs against 1,801 account labels. Only the
account-level ones are usable for likes, which `enumerate_labeler.py` keeps by
default.

### What a full enumeration actually costs and yields

Enumerating Skywatch end to end: **1,133,558 labels scanned in 32 minutes**
(~595/s), 3,713 retractions applied, yielding **128,587 distinct account DIDs
across 79 label values**. Restricted to the abuse vocabulary that is 111,602
accounts. Largest sets:

| Label | Accounts |
| --- | --- |
| bulk-following | 67,874 |
| platform-manipulation | 18,326 |
| suspect-inauthentic | 13,209 |
| inauthentic-fundraising | 9,428 |
| engagement-abuse | 8,883 |
| spam | 7,729 |
| follow-farming | 1,362 |

That is a one-time cost. Incremental refresh from a stored cursor is seconds,
and the resulting file is small enough to ship to `jetstream_ingest` the same
way the existing blocklist export is shipped.

**Caveat: enumeration appears incomplete.** Scoring our ingested authors
against the enumerated set gives 1,133 authors / 5.96% of posts, where the
targeted per-DID sweep found 1,367 / 6.52% — the enumeration recovers 83% of
what direct queries return. There is also a stability signal worth chasing: a
20-page wildcard scan and a 700-page scan, both starting from no cursor,
ended on cursors that were not consistent with each other, which suggests
`uriPatterns=*` ordering is not stable across requests and a cursor walk can
skip records. Before depending on enumeration as the source of truth,
reconcile it against a targeted sweep, or use `subscribeLabels` — whose
cursor is a sequence number by specification — instead.

**The bigger lever is still language.** `record.langs` is already in the
payload, covers 26.9% of posts, and would be a genuinely selective indexed
attribute — unlike any spam signal here, it is large enough to change what the
retrieval corpus looks like.
