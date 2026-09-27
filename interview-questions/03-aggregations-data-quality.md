# Aggregations and Data Quality

Practice module: `src/main/scala/aggregations` (03) and `src/main/scala/advancedAggregations` (06).

## Q1. "Give me total sales per customer AND a grand total row in one query."

**Expected:** `ROLLUP(city)` gives per-city + grand total (NULL for rolled-up key).

```sql
SELECT COALESCE(cust_id, -1) AS cust_id, SUM(amount) AS total
FROM orders
GROUP BY ROLLUP(cust_id)
```

**Probes:**
- **ROLLUP vs CUBE vs GROUPING SETS**: ROLLUP = hierarchical subtotals left→right; CUBE = every combination; GROUPING SETS = explicit list. CUBE(2 cols) = 4 groups; ROLLUP(2 cols) = 3 groups.
- Ambiguity of NULL city: real value vs rollup subtotal → use `GROUPING(city)` / `GROUPING_ID` to distinguish, not `COALESCE`, because a legit `NULL` city and the subtotal both show NULL.
- Where a full `GROUPING SETS` intent surfaces in interviews: "show weekly totals AND monthly totals in one result set".

---

## Q2. Pivot a status column into columns. (Spark SQL has no PIVOT keyword.)

**Two idiomatic options:**
1. **DataFrame API:** `.groupBy("cust_id").pivot("status", Seq("Completed","Pending","Cancelled")).agg(sum("amount"))`.
2. **SQL emulation with CASE WHEN** (works everywhere):
```sql
SELECT cust_id,
       SUM(CASE WHEN status='Completed' THEN amount ELSE 0 END) AS completed,
       SUM(CASE WHEN status='Pending'   THEN amount ELSE 0 END) AS pending
FROM orders GROUP BY cust_id
```

**Probes:**
- Always pass the explicit value list to `pivot` — an implicit distinct () triggers an extra scan and unbounded cardinality in columns.
- Un-pivot: `stack()` or `explode(map(...))`.

---

## Q3. Compute the median — Spark has no built-in MEDIAN().

**Expected:**
- `percentile_approx(amount, 0.5)` for large data (approximate, cheap, `approxQuantile` in the DF API).
- Exact median: `percentile_approx` with `accuracy` tuned up, or sort values materialize; or `PERCENTILE_CONT` on Hive staging (loses Spark portability).
- Median *per group*: `SELECT status, percentile_approx(amount, 0.5) FROM orders GROUP BY status`.

**Probes:**
- Why approximate? Exact percentile requires sorting the whole group per task; approx uses a fixed-size histogram (TDigest), constant memory.
- `approx_count_distinct` vs `count(DISTINCT ...)` — same trade-off, `approx_count_distinct` uses HLL.

---

## Q4. A dashboard shows total revenue that doesn't match the nightly reporting table.

**Possible causes (all common):**
1. **Time zone drift** — one pipeline casts `TIMESTAMP` in `UTC`, the other in local time; the day boundary shifts. Fix: store `date`/`ts` in a single convention, apply TZ once at the boundary.
2. **Null/duplicate handling differs** — count(*) vs count(non_null), or the raw table hasn't been deduped while the curated one has.
3. **Partition lag** — dashboard reads the current partition; the nightly job reads `<= today` or `<= yesterday`. Off-by-one day truncation.
4. **Approx functions in one path** (`approx_count_distinct`) vs exact in the other.
5. **Late/Duplicate data in streaming** reprocessing.

**Answer structure for interviews:** define what "correct" means, compare both
queries' predicates and column selection, then diff with a `MINUS`/`EXCEPT`
query at the row level to find where counts diverge.

---

## Q5. Mutating a Spark DataSource read — you need the previous day's aggregate to compute today's.

**Expected:** window or self-join with an offset; or incremental aggregation using
a Delta/Iceberg `MERGE` into a pre-aggregated table keyed on (dimensions, day),
updating running totals. Live demo: `advancedAggregations` cube/rollup/collect.

**Probes:**
- "chain aggregation" — feed today's pre-aggregate into yesterday's total to avoid rescanning history (incremental aggregation / `merge` on the agg table).
- `collect_list`/`collect_set` ordering is not guaranteed → use `collect_list` in a window ordered by ts for ordered arrays (e.g. a path or event sequence).