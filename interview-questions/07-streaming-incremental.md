# Streaming and Incremental Data

You're likely to be asked Streaming concepts even in "batch" roles, since
FAANG pipelines are increasingly streaming-first.

## Q1. In Spark Structured Streaming, how do you guarantee exactly-once on Kafka → S3 (Delta)?

**Answer:**
1. **Offset tracking in checkpoint** — checkpointed offsets feed the Sink, requiring a **WAL-like** location: checkpoint directory + `commit` marks. Only by combining **idempotent sink** + **replayed source offsets** do you get exactly-once.
2. **Idempotent sink** — writing append/overwrite to a partitioned object store keyed on `(batchId, ...)` so replay doesn't duplicate; or Delta `MERGE`.
3. `OutputMode.Append` + `trigger(Once())` for batch-style semantics.
4. Repartition to a single file per partition per micro-batch for `OPTIMIZE` benefits.

Follow-up: why is **Kafka source offset + partition ordering** needed? Because a
failed batch may re-run after a partial commit.

---

## Q2. "Watermark — what happens to late events?"

- `withWatermark("ts", "10 minutes")` vs `event-time processing`: late events within the lag are still included in the window; events beyond the watermark are dropped with `append`/`update` mode if the sink is not stateful enough to accept them.
- A window `GROUP BY window(event_ts, '1 hour')` closes only when the watermark passes **window end** → duplicate/late windows reopen?? No — the window is closed and dropped only when `current watermark >= window end`; a late event that was for a closed window is dropped.
- Practice mental model: watermark = "the engine will not see events with event-time older than this". 

---

## Q3. Streaming join streaming — memory and timeframe pitfalls.

- State is kept for the **join period** configurable via `watermark` + `time constraint`: `A.join(B).where(A.ts + watermark >= B.ts)`.
- **FOO: excessive retention** → remove state; `joinStateStoreRetention` controls cleanup on the stateStore. 
- Verify with Spark UI "State" metrics, and drain to a compacted state table for very long windows (Kafka Streams-style).

---

## Q4. "Backfill": you have months of historical S3 data and a new streaming job must reprocess it.

**Expected:**
- Batch mode replay: set the source to read all historical partitions (`readStream.format("cloudFiles")` or `HDFS dir pattern`), with watermark off; or use **Delta time travel** — simplest is `spark.read.streaming.format("delta").option("timestampAsOf", ...)` but note readStream on old snapshots isn't always supported → do a batch `MERGE`/full replace for the historical window, then start streaming from the current offset.

**Key interview point:** know that **offset can't move backwards** on Kafka — a replay to consumers needs a new ConsumerGroup or a time-based dispatch; S3 source replay is free (just read the directory again).

---

## Q5. Exactly-once review, `OutputMode` comparisons.

| OutputMode | When allowed | Result |
|---|---|---|
| Append | Aggregations without time window | new rows only |
| Update | Aggregations with watermark | updated results per key |
| Complete | Aggregations without watermark | full result per trigger, memory-heavy |

Always mention **dropStale/lateDrops**: `dropDuplicates("ts", "key")` bounds
memory using watermark.