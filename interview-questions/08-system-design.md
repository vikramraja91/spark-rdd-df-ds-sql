# System Design and Architecture

These are the "paint the whole pipeline" questions. FAANG data engineers love
them because they test breadth plus judgment, not just syntax.

## Q1. Design an event analytics pipeline: raw events to a dashboard showing DAU/MAU, top events, funnel.

**Walkthrough (funnel of answers):**
1. **Ingest** — Kafka topics by event type, partitioned by key (usually `user_id`), retention 1–7 days.
2. **Landing** — Spark Streaming (5-min micro-batch) or nightly batch, landing as Parquet partitioned by `event_date` (or Delta/Iceberg for SCD).
3. **Schema / quality** — a validation layer: required `ts`, `user_id` not null; malformed → bad-records path; schema versioned.
4. **Processing**:
   - DAU = `COUNT(DISTINCT user_id)` per day, materialized per partition — **approx_count_distinct** if daily cardinality is huge (practice: `advancedAggregations` / `aggregations`).
   - Top events by window → `ROW_NUMBER()` per hour (practice: `windowFunctions`).
   - Funnel = 4-5 event-specific `CASE WHEN`-based sessionization; session windowing with gap > 30min (see `02-window-and-analytics.md`).
5. **Serving** — pre-aggregated daily tables (star schema) served by a Presto/Trino/ClickHouse-compatible engine; dashboard reads summaries only, never the raw table.
6. **Late data / dedup** — dedupe by `(event_id)` using `ROW_NUMBER` over `ts` on read of the landing layer.

**Interviewer probes:** Exactly-once story, shape skew (`user_id=0`), high-cardinality counts, backfill story, cost (object storage vs provisioned).

---

## Q2. Lambda vs Kappa architecture — which do you pick and when?

- **Lambda**: speed layer (streaming) + batch layer (recompute) + serving. Pros: fresh results, recompute-correct. Cons: two codebases to keep aligned.
- **Kappa**: single streaming path; batch is just replay of the log with a bigger window. Pros: one codebase. Cons: streaming code must be as robust as batch; long replay on stateful operators is operationally the same hazard as Lambda's dual brain.

**Weakness to name:** Lambda's "same logic twice" sync problem; Kappa's dependency on stream processor state retention. Mention when you'd deviate: regulatory recompute mandates a batch path (Lambda has advantage); modern lakehouse (Delta `MERGE`) reconstructs history much cheaper — you get recompute for free on a single pipeline.

---

## Q3. How would you build a CDC (change data capture) pipeline into a warehouse?

1. **Capture**: Debezium reading the source DB binary log → Kafka topic per table (upsert semantics, key = primary key).
2. **Transport**: topics partitioned by PK; compacted topic retains latest per key.
3. **Land**: Delta/Iceberg table with `MERGE ON pk` on the batch/flink connector — eventually consistent with the source within the microbatch lag.
4. **Handle orderings**: tombstone (soft delete) via `opType`; sequence checks via `ts` + LSN to ignore out-of-order updates.
5. **Latency choice**: 10-min microbatch dumps vs continuous — trade SLA vs cost.

**Probe:** "How do you backfill a new target column (the column is added upstream)?" → schema on write merges, historical rows are NULL → run `MERGE` from a snapshot table + `UPDATE` on the evolving partition.

---

## Q4. Your distributed ML feature store needs yesterday's features delivered in <5 minutes after midnight each day. Batch or streaming?

**Answer: moderate** — lambdas split:
- Features that don't need instant freshness → batch T-1 recompute, delivers at 00:05.
- Features that affect ranking now → streaming feature (sliding windows) incrementally.
Call out SLA-driver: 5-minute latency generally belongs to a Kappa-style streaming
path (watermark + trigger), not a nightly batch. But for "always the previous day's
completed window", a batch job + partition-aligned read beats a stream that must
both close and deliver under that deadline.

---

## Q5. "A VP asks: why do we need a lakehouse at all vs just Spark on Parquet?"

**Answer structure (touch the 4 pillars):**
1. **ACID on table-level writes** — concurrent writers to the same directory no longer corrupt it; `MERGE`, `VACUUM`, time travel.
2. **Schema enforcement + evolution** — nullable constraints, ADD COLUMN auto.
3. **Delete/update at scale** — parquet files are immutable; lakehouse rewrites only affected files.
4. **Compatibility** — reads from Spark + Presto + Flink at once.
**Trade-off**: extra metadata layer, compaction costs.

---

## Q6. You have a 1TB order table and need "top N buyers per country + total per country". Sketch the physical plan.

1. Filter/Push: `WHERE order_date >= ?` (partition pruning) — avoid scanning all history.
2. Plan: performance-wise, `GROUP BY country, buyer` (shuffle by country+buyer), then country rank window or per-country top-N with `ROW_NUMBER` (see `windowFunctions`).
3. Optimization to name: reduce early via `approx_count_distinct` when raw counts not needed, bucketed tables on `country` avoid a join reshuffle.
4. Serving: materialize per-day top-N table (append-only) rather than rescanning daily.

**Follow-ups:** You'll get "What if 'country' is skewed (e.g. US = 60%)?"→ salt or
broadcast the country dimension on a hash, or two-pass (per-country top 10k from
each partition first, then merge).