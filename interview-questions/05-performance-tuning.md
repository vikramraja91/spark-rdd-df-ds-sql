# Performance Tuning and Optimization

Practice module: `src/main/scala/optimizationCaching` (12) and `readWriteFormats` (10).

## Q1. A Spark job is slow. Walk me through how you diagnose it.

Interviewers want a **systematic order**, not a guess:

1. **Spark UI first**: which stage dominates wall-clock time?
   - *High shuffle read/write* → skew or unnecessary shuffles.
   - *One or few long tasks* → skew.
   - *Task time uniform but GC heavy* → memory/C-strings issue.
   - *Few tasks = underutilization* → parallelism (few partitions).
   - *Spill to disk* → executor memory too small for buffers (shuffle spill or sort spill).
2. **Explain plan**: is the predicate pushdown happening? broadcast join chosen? Is there a full scan when partition pruning was expected?
3. **Data-level**: distribution of keys, cardinality, null rates, file count/size.
4. Then apply the fix (Q2), re-run, confirm improvement via the UI.

---

## Q2. What's your tuning checklist (in order of impact)?

1. **Query/Optimization (biggest, free):**
   - Push filters down (`WHERE` before join), avoid `DISTINCT+ORDER BY`, avoid `COUNT(DISTINCT)` on hot UDFs, use `approx_count_distinct` where semantics allow.
   - **AQE on** (default Spark 3): `optimizeSkewedJoin`, `coalescePartitions`, `optimizeLocalShuffleReader`.
   - Join hints to fix bad planner choices / skew (`BROADCAST`, `SHUFFLE_HASH`, `MERGE`).
2. **Storage:**
   - Columnar formats (Parquet/ORC), predicate pushdown, partition on the filter key, bucket on join keys.
   - Right-size files (avoid 100k tiny files) — read *and* write side.
3. **Cluster/DAG shape:**
   - `repartition` parallel, `coalesce` to consolidate; `cache` only for reuse across actions, unpersist aggressively (see 12).
4. **Memory:**
   - `spark.executor.memory`, overrides `spark.memory.offHeap`, `spark.sql.shuffle.partitions`, `spark.sql.adaptive.coalescePartitions.minPartitionSize`.
5. **Codegen/cost:** avoid UDFs at hot paths (opaque to optimizer); prefer built-ins and higher-order functions.

**Important:** start at the top; tuning `spark.executor.memory` without fixing a
redundant shuffle is polishing the wrong thing.

---

## Q3. "Why is my join slow when both sides are only 1GB but the cluster has 200GB?"

Common reasons:
- Both sides unbucketed → SortMergeJoin requires full shuffle of *both* sides on the join key, even if small.
- The 1GB figure is compressed *on disk*; the in-memory row representation plus serializer overhead is much bigger — and if the small side is still under the broadcast threshold it may already be a 1-task bottleneck.

**Fixes:** Bucket both tables on the join key with the same bucket count (bucket join → no shuffle), or bump the broadcast threshold / hint broadcast.

---

## Q4. OOM on an executor — walk through the causes.

1. Large `GROUP BY` with many distinct keys → hash aggregation memory (`spark.sql.autoBroadcastJoinThreshold`, `spark.sql.shuffle.partitions` tiny).
2. **Cache a large dataset** in `MEMORY_ONLY` while it doesn't fit, Spark evicts → but if it can't fit and spills, GC spikes.
3. **Broadcast variable too large** — every executor holds a copy; many small executors amplify it.
4. **Huge shuffle buffers** for a skewed join reducer.

**Fixes:** smaller partitions + `spark.sql.shuffle.partitions`; `persist` with `MEMORY_AND_DISK_SER`; replace `collect`/broadcast of huge lookup with a bucketed join; raise `spark.sql.adaptive.skewJoin` handling.

---

## Q5. Explain Adaptive Query Execution (AQE) — three optimizations and when they kick in.

1. **`coalescePartitions`** — after the shuffle, reduce reducers when output is small → fewer, larger tasks.
2. **`optimizeSkewedJoin`** — detects skewed shuffle partitions and splits them into better-sized pieces (task-level parallelism).
3. **`optimizeLocalShuffleReader`** — for a stage with a shuffle from a map-side filter, reads only the needed shuffle partitions (an extra stage coalesces to read only necessary files).

These all operate at *runtime* using the previous stage's metrics — which is why the
UI's "Adaptive Execution" section shows the reassigned plan.

---

## Q6. "The job runs but sparks.job.done comes instantly — nothing got computed!?"

**What's happening:** everything was pushed down into a single **not executed** plan —
the action was on an *empty* operation (e.g. `df.limit(0)`, or a `collect` on a lazily computed empty base that returns instantly) and no stage ran.

**Follow-up:** confirm whether the query planner pushed the filter to a file/metadata
(columnar predicate pushdown of `WHERE FALSE`), which is correct but surprising.
Demonstrate with `.explain()` on `WHERE 1=0`.