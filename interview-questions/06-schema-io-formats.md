# Schema Evolution and I/O Formats

Practice module: `src/main/scala/readWriteFormats` (10), `src/main/scala/dateStringFunctions` (13), `src/main/scala/nullHandling` (11).

## Q1. A new column was added to the upstream Parquet files. Reads fail or miss it.

Volume of the scene: schema evolution for columnar formats.

**Parquet/ORC:** `spark.read` safely merges schemas (`mergeSchema` option, or by
default uses the *latest* file's schema for reads). In Spark 3, the DataFrameReader
merges by default if the files diverge. When schema mismatch breaks a job, common
root causes:
- **Type changed** (int → long/string) across partitions: merge picks a common type or the write-time variance is rejected.
- **Nested struct added a field**: read it via dotted access (`df("a.b")`) and fill missing with `lit(null)`.
- **Missing partition variable**: reading a `partitioned` directory with a new partition column needs `partitionBy` on read.

**Fixes:**
- Explicit schema on read with only the columns you need.
- `columnOfInterest` null-handling: `COALESCE(new_col, 'default')` for late-arriving columns (practice in `nullHandling`).
- For Delta/Iceberg: `ALTER TABLE ADD COLUMNS`, `MERGE` provides **automatic schema evolution** on write — mention this as the production answer for ADD COLUMN without breaking downstream.

---

## Q2. Compare Parquet vs ORC vs CSV vs JSON for a warehouse.

| Format | Compression | Schema | Predicate pushdown | Best for |
|---|---|---|---|---|
| Parquet | columnar + snappy/zstd, great | embedded | yes | analytics default |
| ORC | columnar, often better in Hive | embedded | yes | Hive/ore schedules |
| CSV | poor | none (infer) | no | import/export, legacy |
| JSON | poor | none (infer) | no | nested event dumps, streaming |

**Probes:**
- **Parquet zero-copy + projection**: reading 2 columns of a 200-column table reads only those column chunks — this is columnar storage for *predicate* AND *projection* pushdown.
- JSON `multiline`, `dateFormat` options; quoting/escape nuances on CSV.

---

## Q3. "Reading a CSV: inferSchema on — how do you know the types are right?"

**Answer:** you don't reliably. `inferSchema` examines a sample (default `samplingRatio=1.0` for local, configurable), and low-cardinality numeric/date-like strings can mis-infer (e.g. small int. vs long). Always:
1. Provide an explicit `StructType` schema for prod reads.
2. Add a `DATE`/`TIMESTAMP` cast at ingest boundary in a single TZ convention (see `dateStringFunctions`).
3. Use `badRecordsPath` to capture malformed rows instead of silently dropping.

---

## Q4. A job writes 40k tiny Parquet files and everything slows down.

**Root cause:** writing `df` as-is — one file per partition/task → HDFS/object-store metadata explosion, slow `listFiles`, bad reads.

**Fixes:**
- `df.coalesce(n)` / `repartition(n)` before write based on target file size (`~128MiB` target for Parquet).
- Enable AQE `coalescePartitions` on write side.
- Use single write for large tables: `df.write.option("maxRecordsPerFile", ...)`.
- Bucketed table writes keep balanced file sizes (see `readWriteFormats.bucketBy`).

---

## Q5. Idempotent incremental load from an external source (e.g. an API) into a lake table.

**Expected answer:** a wrapper approach:
1. Read the whole target partition (date/file pattern), merge with incremental records in memory (dedupe window over `update_ts`), rewrite the affected partition. Bad for big partitions.
2. **Delta/Iceberg/Hudi** `MERGE ON key WHEN MATCHED UPDATE ELSE INSERT` — atomic, no overwrite race.
3. **Rewrite-aware**: write incremental to a "pending" table, then `MERGE` — journal of what's converged.

Probe: what if source has duplicates or late updates? → dedupe by `ROW_NUMBER` on
`(key, update_ts)` before `MERGE` (see `windowFunctions`), truncate on partition write.