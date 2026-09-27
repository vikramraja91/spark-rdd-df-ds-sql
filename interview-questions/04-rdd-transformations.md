# RDDs, Transformations, and Partitioning

Practice module: `src/main/scala/rddTransformations` (07), plus `filteringAndSorting` (02).

## Q1. "What's the difference between RDD, DataFrame, and Dataset? Which would you use and why?"

**Short version (interviewer wants this crisp):**

| Aspect | RDD | DataFrame | Dataset |
|---|---|---|---|
| Typing | compile-time | runtime (Row) | compile-time (case class) |
| Optimization | executor topologies only | Catalyst optimizer + codegen | Catalyst + codegen (typed ops) |
| Serialization | Java/Kryo serializer | Encoders (binary, compact) | Encoders + compile-time types |
| Best for | custom logic / low-level control | most analytics | type-safe compile-time checks |

Follow-ups:
- DataFrames and Datasets both compile to the *same* Catalyst plan; `Dataset[T]` adds compile-time type safety while retaining the optimizer.
- Whole-stage codegen: Catalyst turns `map/filter/select` expression trees into one JVM method — the reason DataFrame/Dataset often beat hand-written RDD `map`.

---

## Q2. groupByKey vs reduceByKey vs aggregateByKey — when does each produce a shuffle?

- **groupByKey**: shuffle *all* values for a key to one reducer, then you reduce on the executor. No map-side combine → the biggest shuffle.
- **reduceByKey**: map-side combine happens *before* the shuffle (same-type combine function) → far fewer bytes shuffled.
- **aggregateByKey**: lets the *accumulator type differ* from the value type (e.g. values are ints, accumulator is a custom struct; or keyed map accums). Also does map-side combine via the `seqOp`.

**Probe:** "What does map-side combine mean physically?" → each map task reduces locally before writing shuffle files.

---

## Q3. coalesce vs repartition — which is cheaper and why?

- `repartition(n)` always shuffles; `repartition(n, key)` re-partitions by hash of key (can help colocate a later join).
- `coalesce(n)` merges existing partitions without a shuffle (only when n < current) — cheaper, but **still moves data over the network if it re-routes** (it keeps partition affinity when possible).
- Rules of thumb: shrink with `coalesce`, rebalance/grow with `repartition`; `coalesce(1)` before a small write avoids a partition-per-file explosion.
- Driver caution: `.repartition(1)` concentates all data into the driver's output for collect — OOM when collecting huge results.

---

## Q4. Accumulators vs broadcast variables — what are they for?

- **Broadcast**: a read-only lookup shipped once per executor, shared by tasks on that executor (avoids re-serializing it into each task closure). Ideal for small dimension tables / config.
- **Accumulator**: driver-owned counters updated only *inside* actions (e.g. count of invalid records seen during `foreach`). LongAccumulator/DoubleAccumulator are the typed ones; implement `AccumulatorV2` for a custom aggregate type.
- Trap: accumulators are **not reliable for data pipeline results** — they update every retryable attempt (task rerun on a failure re-applies the update twice). Counters for observability only.

---

## Q5. Sort an RDD by value without a full `sortBy` — how does `sortByKey` work?

- `sortByKey` uses a **RangePartitioner**: a sample pass estimates key ranges, then a second pass rrange-partitions so each reducer gets a contiguous key range, reducing shuffle size vs sorting everything in-memory.
- Interviews often ask: "sort descending with `sortByKey`" → `mapValues` the key or use `Ordering` reverse; on a pair RDD, `.sortBy(_._2, ascending=false)`.
- Import ordering: `sortByKey(ascending = false)`.

---

## Q6. What is a narrow vs wide (shuffle) transformation?

- **Narrow**: `map`, `flatMap`, `filter`, `mapPartitions`, `union` — each output partition depends on at most one input partition; no shuffle; failure needs recomputing only that partition.
- **Wide**: `groupByKey`, `reduceByKey`, `join`, `distinct`, `repartition` — shuffle; lineage recompute after a failed wide stage re-shuffles, so checkpoint long chains (see `optimizationCaching` checkpoint demo).
- Follow-up: **why checkpoint in production** → truncate lineage/DAG for recompute-bound jobs, and evict old shuffle files.

---

## Q7. A job that worked on an RDD with `mapPartitions` now OOMs on the driver.

**Typical cause:** `foreach`/`collect` — `collect()` pulls the *entire* resulting RDD onto the driver, and the driver can't hold it. `foreach(println)` on a huge RDD also dumps everything to the driver's stdout.

**Fix:** if you only need results sampled, `take`, `top`, or aggregate on executors first; write the output to a file instead of collecting. Practice: 07's dataset/mapGroups sections avoid collect-then-print.