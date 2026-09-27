# Joins, Skew, and Data Distribution

Practice module: `src/main/scala/joins` (04) and `src/main/scala/rddTransformations` (07).

## Q1. A nightly join job runs fine until recently, then a few tasks take 10x longer than the others.

**What's happening:** data skew — a small number of join keys have far more rows
than the average (e.g. `user_id = 0`, a default/unknown value, a hugely popular product).

**How you'd verify:**
- Spark UI: look at the shuffle stage — a few "long tail" tasks with much higher
  `Shuffle Read Size / Records` and duration than the median.
- Query a breakdown of the key distribution: `SELECT key, COUNT(*) FROM t GROUP BY key ORDER BY 2 DESC LIMIT 10`.
- Confirm the join is SortMergeJoin (not Broadcast).

**Fixes (from cheapest to most involved):**
1. **Filter/isolate hot keys first** — handle the "unknown" bucket separately or
   exclude it from the join.
2. **Broadcast the small side** — `/*+ BROADCAST(small) */` so no shuffle happens
   at all (threshold: `spark.sql.autoBroadcastJoinThreshold`).
3. **Salting** — for the skewed key, append a random suffix `0..N` on the large
   side and replicate the small side `N` times, then join on the salted key. This
   spreads the hot key across `N` reducers. See `optimizationCaching.SparkPractice`
   (12) for a worked example.
4. **AQE skew join** — Spark 3 enables `spark.sql.adaptive.skewJoin.enabled`
   (default on) which dynamically splits hot partitions at shuffle time. Turn it
   on and confirm via the UI that the skew partition was split.

**Follow-ups interviewers ask:**
- When does a broadcast join stop being a win? → When the small side's broadcast
  memory + JVM overhead exceeds the gain; watch for OOM on executors or slowed
  GC because every task gets a copy of the table.
- Why does skew hurt shuffler writers but mostly the reducer? → The map side
  writes proportional to total data; the reducer task that pulls the huge
  partition becomes the bottleneck.

---

## Q2. An inner join returns too many rows for one key. The result is wrong.

**What's happening:** one-to-many (or many-to-many) semantics — duplicates on
either side that the business expects to be unique. e.g. `orders ⋈ customers`
duplicates customer rows for every order.

**Verify:** 
- Count duplicates per key on each input: `SELECT key, COUNT(*) c FROM side GROUP BY key HAVING c > 1`.
- Compare pre-join vs post-join row counts.

**Fixes:**
- Deduplicate first on the many side: `ROW_NUMBER() OVER (PARTITION BY key ORDER BY recency/amount DESC) = 1`
  to keep the latest/most relevant row (see `windowFunctions`, 05).
- Verify business definition: should it be a `LEFT SEMI` / `LEFT ANTI` join instead of inner/left?
- For "latest CDC row per key", read the most recent timestamp per key before joining (slowly changing dimension resolution).

---

## Q3. You must join two 10TB tables. Which join strategy do you pick and why?

**No single answer — it depends:**
- **Broadcast join** if either side fits comfortably in executor memory (< broadcast threshold). No shuffle, best for star schemas.
- **Sort-merge join** (default) for equal-bucket / full-table joins. Sorting both sides then merging streaming — scales to very large data, but does a full shuffle.
- **Shuffle hash join** for medium tables where one side can build a hash table per partition (often forced with a hint when the planner picked sort-merge sub-optimally).
- **Bucketed + sorted tables** — if both tables are bucketed on the join key (same bucket count), Spark can do a **bucket join** with zero shuffle. See `readWriteFormats.bucketBy` (10).

**Key interview point:** the answer is "it depends on size, cardinality, skew, and
distribution," and you should look at the actual physical plan before tuning.

---

## Q4. A LEFT JOIN returns rows with NULL nulls on the right side where you expected matches.

**What's happening:** usually NULL or type-mismatched join keys.

**Common causes:**
1. **Null keys**: SQL equality `NULL = NULL` is NULL (falsy), so null keys never match — _null left side may still appear_ (LEFT join preserves it).
2. **Type mismatch** (int vs long, string vs int) — Spark coerces only certain combinations; a string `"1"` vs int `1` may not match depending on how the plan coerces.

**Fixes:**
- `COALESCE(key, -1)` on both sides if you want nulls to join together (and be sure the sentinel can't collide).
- Cast explicitly to a common type before the join.
- Investigate trailing whitespace / casing on string keys (`TRIM`, `LOWER`).

Practice: `nullHandling` (11) — join null semantics.