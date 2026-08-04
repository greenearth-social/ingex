# Two-Tower kNN: CPU/RAM Assessment and the Quality Corpus

**Status:** Implemented · 2026-08-04
**Issue:** [ingex#442](https://github.com/greenearth-social/ingex/issues/442)
**Related:** [api#338](https://github.com/greenearth-social/api/pull/338) (medium-term datastore redesign) · [api#343](https://github.com/greenearth-social/api/issues/343) (attribution telemetry) · [api#310](https://github.com/greenearth-social/api/issues/310) (kNN profiling) · [api#324](https://github.com/greenearth-social/api/issues/324) (window-cap removal)

---

## 1. Summary

Two-tower kNN is the bottleneck behind our degraded/failed feed rate. ingex#442
asked two questions: is ES CPU/RAM the binding constraint, and if raising it
won't fix things on its own, build a `posts_recent_quality` corpus.

**Answer: CPU and RAM are both saturated, but they are symptoms. Raising either
buys a linear improvement against a problem that is algorithmic, so neither
fixes it on its own.** The corpus is the fix, and it is implemented here.

A modest CPU bump is still worth doing as headroom (§5), but it is a safety
margin, not the remedy.

---

## 2. Is CPU/RAM the bottleneck?

Current prod data tier (`index/deploy/k8s/environments/prod/kustomization.yaml`):
4 nodes × 48Gi RAM (8Gi JVM heap, so ~40Gi of page-cache budget each),
7385m CPU, 2048Gi disk — roughly 29.5 vCPU and 160GB of aggregate cache against
a ~5.4TB store.

Both resources are genuinely pinned, in two different regimes. Measurements from
the 2026-07-31 load-test ladder and probe series (api#338 §2.1):

| Regime | Evidence | Reading |
|---|---|---|
| **Throughput — CPU bound** | At 605 renders/min all four data nodes sit at **84–99%** for 5 minutes; search thread pool 13/13 active, queue depth to 103, **zero rejections**; device reads **flat** (59 MB/s vs 42–52 idle); api meanwhile at 10 instances and 44% CPU | A 40× rise in search load produced no rise in disk reads. The marginal cost of query volume is CPU spent on vector distance computations, and the ceiling is the shared cluster, not our service |
| **Tail latency — residency bound** | The byte-identical probe query costs p50 **1188ms** while background IO churns the cache vs **242ms** when it does not (**4.9×**). No other generator moves by more than 0.25×, and the largest moves the wrong way | Page cache per data node swings 2–40GB against a ~1.35TB store — 1–3% residency. two_tower is the only cache-sensitive workload in the system |

Note that failure here is **queuing, not rejection**: ES silently queues while
the api's 4s generator timeout fires client-side, and then keeps executing the
abandoned query to completion.

### 2.1 Why both symptoms have one cause

`two_tower` applies `like_count>=20` as a pre-filter inside the kNN clause
(`api`: `lib/candidates/two_tower.py`, `lib/candidates/es_candidates.py`). That
predicate matches ~4.6% of `posts_recent`, which trips **both** of Lucene's
filtered-kNN fallbacks:

- segments where the filtered count is ≤ `num_candidates` (currently
  `max(100, k·10)`) bypass the HNSW graph outright; and
- on larger segments, traversal exceeds a visit budget equal to the filter
  cardinality, discards the work it has done, and exact-scans instead.

`profile:true` on prod measured **~460k vector operations per query** against
~928k matching documents (api#310) — the signature of an exact scan, and exactly
the `vector_ops_count > 50k` symptom the api's profiling runbook documents.
`TWO_TOWER_MAX_AGE_CAP_HOURS = 96` exists for no other reason than to bound that
scan to ~320k vectors, trading recall for scan size.

So: the CPU is burned scanning, and the scan is random-access across gigabytes
sharing a page cache with ~4.5TB of documents and a continuous write stream.
One mechanism, two symptoms.

### 2.2 Why scaling CPU/RAM does not fix it

- **CPU.** HNSW on this corpus is roughly 12–25k distance computations, against
  ~320k vectors scanned today — a 15–25× gap. Closing it with hardware means
  growing the data tier from 4 nodes to something like 60–100. That is not a
  viable answer to a cost-minimisation problem.
- **RAM.** 160GB of aggregate cache against a ~5.4TB store is ~3% residency;
  making the corpus resident needs ~34× the RAM. Making only the *vector working
  set* resident is the actual goal — and a small dedicated index achieves that
  for free.

---

## 3. The quality corpus

`posts_recent_quality` holds only posts at or above the like threshold, within a
two-week window. Because **membership is the predicate**, `like_count>=20`
matches ~100% of that index instead of ~4.6%. Neither Lucene fallback triggers,
and the graph search is used again.

Two consequences, corresponding to the two regimes in §2:

- **CPU:** graph traversal instead of exact scan — the 15–25× reduction above.
- **Residency:** the working set drops from a cache shared with ~4.5TB to
  ~773k documents. The vectors alone are ~99MB of int8; the lean documents
  (§3.1) keep the whole index comfortably resident, and it self-warms because
  the searched set is identical for every user.

It also removes the justification for `TWO_TOWER_MAX_AGE_CAP_HOURS`, which
unblocks api#324. The cap is deliberately left in place here so that the index
switch lands as a single variable.

### 3.1 Why not simply stop pre-filtering (api#338 E.9)

api#338's Appendix E.9 raises the alternative of moving `min_like_count` out of
the kNN clause and post-filtering with overfetch, which also restores HNSW. The
dedicated corpus is preferable in the short term for one specific reason:
**it does not change what the query returns.**

Post-filtering returns *the popular posts among the nearest*; pre-filtering
returns *the nearest posts that are popular*. Those are different result sets,
and E.9 concedes the difference carries a candidate-quality risk needing a
shadow comparison and a product decision. Restricting the corpus keeps the
pre-filter semantics byte-for-byte identical to today's, so the change is
purely a performance change and needs no A/B to de-risk.

### 3.2 What the index carries

Lean by design — the fields two-tower filters on, plus the fields the api reads
back off a kNN hit (`CANDIDATE_SOURCE_FIELDS`), and nothing else:

- **Vectors:** `ge_post_embedding` only. The api hydrates MiniLM L12 separately
  from `posts_recent` (`api`: `routers/xrpc.py` `_hydrate_embeddings`), so
  copying L12/L6/gemma would multiply the footprint for no read. `embeddings` is
  mapped `dynamic: false` so a stray family cannot be auto-mapped in.
- **Filter fields:** `created_at`, `ge_post_embedding_model_uuid`,
  `contains_video`, `like_count`.
- **Read-back fields:** `at_uri`, `author_did`, `content`, media counts,
  `external_embed`.

`like_count` is a snapshot taken at promotion time and is deliberately **not**
maintained afterwards. It is never a ranking input — both rankers refetch it
from `posts_recent` (`api`: `lib/rankers/heavy_ranker.py`,
`lib/rankers/two_tower.py`) — and as a filter a stale value is safe, because
like counts only grow in the common case, so a stale value at or above the
threshold implies the live one is too. This is what lets promotion be
copy-once rather than a write per like.

### 3.3 Membership mechanics

- **Steady state:** `jetstream_ingest` reads each post's post-update like count
  out of the bulk-update response it already requests (`"_source": true`, so no
  extra round trip) and promotes the posts that **crossed** the threshold in
  that batch — not those merely at or above it. A post crosses once, so
  promotion traffic is a small fraction of like traffic.
- **Bootstrap:** `ingest/cmd/backfill_quality_index` seeds the corpus once per
  environment.
- **Bucketing:** documents go to the period index matching their own
  `created_at`, not the current period, so the map from `at_uri` to index is
  stable and a re-promotion overwrites in place rather than creating a duplicate
  the alias would surface twice.
- **Window:** the alias spans **3** periods while ILM deletes at 14d. Two weekly
  periods would dip to 7 days of coverage just after a boundary; three keeps the
  window at or above the 14 days the retrieval path assumes.

> **The backfill must not use `_reindex`.** The posts template sets
> `_source.excludes: ["embeddings"]`, so `_reindex` — which copies `_source` —
> would produce documents with no `ge_post_embedding` at all, and an index that
> answers every kNN query with nothing. The vectors live only in doc values and
> must be read back via `docvalue_fields`. `fields` does not work either: it
> falls back to decompressing `_source` for `dense_vector` and silently returns
> nothing (api#325, ingex `dd5cbc8`).

---

## 4. Compute cost

The corpus is ~4.6% of the posts corpus over 14 days (~773k documents), carrying
one 128d vector instead of four embedding families and a subset of fields. Set
against not adding ES nodes, this is close to free:

- One primary shard plus one replica in prod — kNN fans out to every shard, so
  fewer shards means fewer HNSW graphs consulted per query.
- Ingest write amplification is bounded by the *crossing* rate, not the like
  rate.
- No new services, no new operational surface.

---

## 5. Recommended CPU/RAM change

Recommended, but as headroom rather than as the fix, and best applied **after**
measuring the corpus switch so the two effects stay separable:

| Setting | Today | Suggested | Rationale |
|---|---|---|---|
| data node CPU | 7385m × 4 | **~11000m × 4** | §2 shows 84–99% at 605 renders/min with queue depth 103. Even with the scan removed, headroom for the other four workloads and for growth |
| data node memory | 48Gi × 4 | unchanged | Residency for the *vector* working set is solved by the corpus. Buying general residency needs ~34× and is not worth it |
| JVM heap | 8Gi | unchanged | Heap is not the constraint; page cache is, and heap competes with it |

Re-run the api#189 ladder after the switch, **outside** the 00:00–03:00 UTC
cold-read storm, and read data-node CPU and search thread-pool queue depth at
fixed QPS — the signals that actually moved in §2, which a latency-only
criterion would miss.

---

## 6. Relationship to api#338

This is the short-term fix and does not replace the Phase 1 proposal. It removes
the exact-scan regime and the residency cliff for two-tower, but it leaves
vector serving on a page cache shared with the document corpus, and it does
nothing for high-frequency partial updates (api#338 §2 row 5), which is the
harder half of that case. It does, however, make the Phase 1 timeline
non-urgent, and the per-`op` ES metrics from api#350 will now measure a corpus
whose cost is dominated by graph traversal rather than by scan size.
