# Spark & Data Engineering Scenario Questions

Spark SQL, DataFrames, Datasets, and RDDs — with the approach interviewers want
to hear. Each file groups scenarios by topic and links to the matching practice
module in `data-engineering-sql-spark-maven`.

How to use this folder:

1. Read a question, try to answer out loud in 1–2 minutes (lay out symptoms,
   root cause, fix, and how you'd verify).
2. Open the linked module in IntelliJ and run each concept to see it live.
3. Rehearse the follow-ups — interviewers drill two levels deeper on the answer
   you give.

## Index

| # | Topic | File |
|---|-------|------|
| 1 | Joins, skew, and data distribution | [01-joins-and-skew.md](01-joins-and-skew.md) |
| 2 | Window functions and analytics | [02-window-and-analytics.md](02-window-and-analytics.md) |
| 3 | Aggregations and data quality | [03-aggregations-data-quality.md](03-aggregations-data-quality.md) |
| 4 | RDDs, transformations, partitioning | [04-rdd-transformations.md](04-rdd-transformations.md) |
| 5 | Performance tuning and optimization | [05-performance-tuning.md](05-performance-tuning.md) |
| 6 | Schema evolution and I/O formats | [06-schema-io-formats.md](06-schema-io-formats.md) |
| 7 | Streaming and incremental data | [07-streaming-incremental.md](07-streaming-incremental.md) |
| 8 | System design and architecture | [08-system-design.md](08-system-design.md) |

## Golden rules for every answer

- **Start with the symptom**: what does the user/job actually see (slow job,
  OOM, wrong result, OOM on driver, task failure)?
- **Isolate root cause** with concrete evidence: Spark UI (stages, shuffle
  spill, task times, GC), explain plan, skew in task durations.
- **Fix at the right layer**: query rewrite (SQL) ⇄ planner (AQE/hints) ⇄
  storage (partitioning, file size, format) ⇄ cluster (resources).
- **Verify**: re-run, compare row counts / metrics, check event timeline.
- **Mention trade-offs**: you don't get points for a fix that trades a shuffle
  for an OOM.