# Window Functions and Analytics

Practice module: `src/main/scala/windowFunctions` (05).

## Q1. Find the top 3 products per category by revenue — but also keep the rank.

**Expected:**
```sql
SELECT category, product, revenue, rn
FROM (
  SELECT category, product, SUM(revenue) AS revenue,
         ROW_NUMBER() OVER (PARTITION BY category ORDER BY SUM(revenue) DESC) AS rn
  FROM sales
  GROUP BY category, product
) t
WHERE rn <= 3
```

**Interviewer will probe:**
- **ROW_NUMBER vs RANK vs DENSE_RANK**: ties. RANK leaves gaps (1,1,3), DENSE_RANK doesn't (1,1,2), ROW_NUMBER is arbitrary among ties. Pick based on business need — "top N" with ties usually wants DENSE_RANK or RANK.
- **NON-DETERMINISM**: with ties, ROW_NUMBER's ordering within a tie is arbitrary — don't use it for "the latest row" unless you also `ORDER BY` a timestamp/ID.
- **Frame default**: `OVER (PARTITION BY x ORDER BY y)` uses `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — with duplicate sort keys the RANGE frame includes ties, which surprises people writing running totals.

---

## Q2. Compare each customer's current order amount to their previous and next order.

**Expected:**
```sql
SELECT cust_id, order_id, amount,
       LAG(amount, 1)  OVER (PARTITION BY cust_id ORDER BY order_date) AS prev_amount,
       LEAD(amount, 1) OVER (PARTITION BY cust_id ORDER BY order_date) AS next_amount
FROM orders
```

**Probes:**
- First/last row → NULL in LAG/LEAD. Use `COALESCE(LAG(...), 0)` or `IGNORE NULLS` variant if needed.
- "Previous order amount excluding the current row": frame `ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING` or use `SUM OVER (ORDER BY ... ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)`.

---

## Q3. Compute a 7-day rolling average of daily signups, and a year-over-year daily comparison.

**Rolling average:**
```sql
SELECT day,
       AVG(daily) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7
FROM (SELECT order_date AS day, COUNT(*) AS daily FROM orders GROUP BY order_date)
```

**Year-over-year:** self-join the daily aggregates shifted by one year
(`t2.day = DATE_SUB(t1.day, 365)`) or use `LAG(daily, 365)` on an exactly-daily
series. Call out: a missing calendar day shifts a LAG offset, so a calendar-gap
table is the robust approach.

---

## Q4. De-duplicate a CDC log keeping the latest version per key.

```sql
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY updated_at DESC) AS rn
  FROM customer_updates
) WHERE rn = 1
```

**Probes:**
- Frame vs full-partition: this is a **single row per partition** — no frame needed.
- **Drop vs filter**: filtering `rn = 1` runs before any shuffle to dupes; there is no shuffle advantage to using `DISTINCT` here.
- Alternative without a window: `GROUP BY` + `max(updated_at)` then re-join to fetch the payload — window is usually 1 job vs 2.

---

## Q5. "Why is my sessionization query so slow?" (lead/lag + CASE summing flags)

Classic gap-based sessionization:

```sql
SELECT user_id,
       ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY ts) AS session_agg
FROM (
  SELECT user_id, ts,
         SUM(IF(ts - LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) > INTERVAL 30 MINUTE, 1, 0))
           OVER (PARTITION BY user_id ORDER BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
  FROM events
)
```

**Where it breaks:**
- `ts - LAG(...) > 30` compares timestamps; must cast to `BIGINT` (epoch) for arithmetic.
- Double window in one query = two full sorts per partition. Deriving `LAG` where the sort key duplicates (engine) is fine, but if partitioned wrong the whole table gets reshuffled.
- Long-running query → make sure partition pruning on `user_id/day` happens before windowing (window functions can't be pushed down past themselves).