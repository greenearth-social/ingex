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
|---|---|---|
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

## If we act on this

The `bot` self-declaration and the `moderation.bsky.app` spam verdict are both
account-level and both cheap to carry: one keyword field on the post document,
or a DID exclusion set applied at ingest. Neither justifies a datastore change
on its own — 4.3% is not a corpus-shaping number.

The language filter is the one that could be. `record.langs` is already in the
payload, would be a genuinely selective indexed attribute, and connects
directly to the kNN restructuring question — unlike the spam labels, it is
large enough to change what the retrieval corpus looks like.
