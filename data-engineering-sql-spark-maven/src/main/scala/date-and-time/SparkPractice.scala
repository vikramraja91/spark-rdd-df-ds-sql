package dateAndTime

import datasets.{Order, PracticeData}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 17 - Dates and timestamps in Spark SQL.
 *
 * Three things to take away:
 *   1. Spark has exactly three temporal types here: DATE, TIMESTAMP (no
 *      timezone) and INTERVAL. Everything else — strings, epoch numbers, local
 *      timezones — is a conversion with a format attached to it.
 *   2. `CAST` and the `to_*` functions differ: `to_date(str)` uses a *default*
 *      format (`spark.sql.legacy.timeParserPolicy` decides whether a bad
 *      string throws or becomes NULL), `to_date(str, fmt)` always uses fmt.
 *   3. Anything that reads the clock (`now()`, `current_timestamp`) is
 *      non-deterministic and is constant-folded once per query, so a partition
 *      loop never sees two different values.
 */
object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("17-date-and-time")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.warehouse.dir", "target/warehouse-datetime")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, ordersRaw, customerUpdates) = PracticeData.load(spark)
  customerAll.createOrReplaceTempView("customers")
  ordersRaw.createOrReplaceTempView("orders")

  val orders: DataFrame = ordersRaw
    .withColumn("order_ts", col("order_date").cast("timestamp"))
    .withColumn("event_month", trunc(col("order_date"), "month"))

  val label = "\n---------- "

  // =====================================================================
  // 1. Casting and parsing — the format is part of the contract
  // =====================================================================
  println(label + "1. Cast vs to_date/to_timestamp")

  spark.sql(
    """
    |SELECT
    |  CAST('2025-01-10' AS DATE)                          AS d1,
    |  CAST('10/01/2025' AS DATE)                          AS d2_wrong,   -- dd/MM vs MM/dd
    |  to_date('10/01/2025', 'dd/MM/yyyy')                 AS d2_right,
    |  to_date('2025/01/10', 'yyyy/MM/dd')                 AS d3,
    |  to_date('2025-01-10 13:45:00')                      AS d4_truncates,
    |  to_timestamp('2025-01-10 13:45:00')                 AS ts1,
    |  to_timestamp('10-Jan-2025 13:45', 'dd-MMM-yyyy HH:mm') AS ts2,
    |  to_date('not-a-date')                               AS bad
  """.stripMargin).show(false)

  // rdd — java.time, no Spark involvement
  orders.rdd
    .map(row => {
      val d = row.getAs[java.sql.Date]("order_date")
      val ts = new java.sql.Timestamp(d.getTime)
      (d, ts, ts.toInstant.atZone(java.time.ZoneOffset.UTC).toLocalDate, ts.toInstant.toEpochMilli, java.time.LocalDate.parse("2025-01-10").plusMonths(1))
    })
    .collect()
    .sortBy(_._1.getTime)
    .take(3)
    .foreach(println)

  // dataframe
  orders.select(
    col("order_id"),
    col("order_date").cast("string").as("as_text"),
    col("order_ts").cast("string").as("ts_text"),
    date_format(col("order_ts"), "yyyy-MM-dd HH:mm:ss").as("formatted"),
    to_date(lit("10/01/2025"), "dd/MM/yyyy").as("explicit_format"),
    col("order_date").cast("int").as("yyyymmdd_int"))
    .orderBy("order_id")
    .show(5, false)

  // dataset — a typed field is a java.sql.Date/Timestamp, not a string
  orders.as[Order]
    .map(o => (o.order_id, o.order_date.toString, o.order_date.getTime, java.time.LocalDate.of(o.order_date.getYear + 1900, o.order_date.getMonth + 1, o.order_date.getDate).plusDays(7)))
    .toDF("order_id", "date_text", "epoch_ms", "plus_7_days")
    .orderBy("order_id")
    .show(5, false)

  // =====================================================================
  // 2. Extractors — everything a report asks for in one projection
  // =====================================================================
  println(label + "2. Extractors")

  spark.sql(
    """
    |SELECT order_id, order_date,
    |  year(order_date)                                    AS y,
    |  month(order_date)                                   AS m,
    |  dayofmonth(order_date)                              AS d,
    |  dayofweek(order_date)                               AS dow,   -- 1 = Sunday
    |  dayofyear(order_date)                               AS doy,
    |  weekofyear(order_date)                              AS woy,
    |  quarter(order_date)                                 AS q,
    |  unix_timestamp(order_date)                          AS epoch_s,
    |  from_unixtime(unix_timestamp(order_date))           AS back,
    |  date_sub(order_date, 3)                              AS d_minus_3
    |FROM orders
    |ORDER BY order_id
  """.stripMargin).show(false)

  // rdd — the same fields off java.time
  orders.rdd
    .map(row => {
      val d = row.getAs[java.sql.Date]("order_date")
      (row.getAs[Int]("order_id"), d.getYear + 1900, d.getMonth + 1, d.getDate,
        d.getDay + 1, d.toLocalDate.getDayOfYear, d.toLocalDate.getDayOfWeek.getValue)
    })
    .collect()
    .sortBy(_._1)
    .foreach(println)

  // dataframe
  orders.select(col("order_id"),
    year(col("order_date")).as("y"),
    month(col("order_date")).as("m"),
    quarter(col("order_date")).as("q"),
    unix_timestamp(col("order_ts")).as("epoch_s"),
    from_unixtime(unix_timestamp(col("order_ts"))).as("back"),
    date_sub(col("order_date"), lit(3)).as("d_minus_3"))
    .orderBy("order_id")
    .show(false)

  // dataset
  orders.as[Order]
    .map(o => (o.order_id, o.order_date.getYear + 1900, o.order_date.getMonth + 1, o.order_date.getTime / 1000))
    .toDF("order_id", "y", "m", "epoch_s")
    .orderBy("order_id")
    .show(false)

  // =====================================================================
  // 3. Truncation and bucketing — a report key must be a *function* of the
  //    value, never a string of it
  // =====================================================================
  println(label + "3. Truncation and bucketing")

  spark.sql(
    """
    |SELECT order_id, order_date,
    |  trunc(order_date, 'month')                     AS trunc_month,
    |  date_trunc('month', order_date)                AS dt_month,
    |  trunc(CAST(order_date AS TIMESTAMP), 'hour') AS trunc_hour,
    |  date_trunc('week', order_date)                AS week_start,
    |  date_trunc('quarter', order_date)              AS q_start,
    |  add_months(order_date, 3)                      AS plus_3m
    |FROM orders
    |ORDER BY order_id
  """.stripMargin).show(false)

  // add_months clamps to the end of the month — the classic surprise
  spark.sql(
    """
    |SELECT d,
    |  add_months(d, 1)          AS plus_1m,
    |  d + INTERVAL 1 MONTH      AS plus_1m_interval,
    |  d + INTERVAL 30 DAY       AS plus_30d
    |FROM (SELECT CAST('2025-01-31' AS DATE) AS d)
  """.stripMargin).show(false)

  // month-over-month with a truncated key (the anti-pattern is comparing the
  // raw date to the truncated one, which only works on month starts)
  val monthly = orders
    .groupBy(trunc(col("order_date"), "month").as("month"))
    .agg(sum("amount").as("amt"), count(lit(1)).as("cnt"))
    .withColumn("prev_amt", lag("amt", 1).over(Window.orderBy("month")))
    .withColumn("mom", round(col("amt") / col("prev_amt") - 1, 3))
  monthly.orderBy("month").show(false)

  // rdd
  orders.rdd
    .map(row => {
      val d = row.getAs[java.sql.Date]("order_date").toLocalDate
      (row.getAs[Int]("order_id"), d.withDayOfMonth(1), d.withDayOfYear(1).withMonth(1), d.plusMonths(1).withDayOfMonth(1))
    })
    .collect()
    .sortBy(_._1)
    .foreach(println)

  // dataframe
  orders.select(col("order_id"),
    trunc(col("order_date"), "month").as("month_start"),
    date_trunc("week", col("order_date")).as("week_start"),
    date_trunc("quarter", col("order_date")).as("q_start"),
    add_months(col("order_date"), 3).as("plus_3m"))
    .orderBy("order_id")
    .show(false)

  // dataset
  orders.as[Order]
    .map(o => (o.order_id, o.order_date.toLocalDate.withDayOfMonth(1).toString, o.order_date.toLocalDate.plusMonths(1).withDayOfMonth(1).toString))
    .toDF("order_id", "month_start", "next_month_start")
    .orderBy("order_id")
    .show(false)

  // =====================================================================
  // 4. Arithmetic — datediff, months_between, intervals
  // =====================================================================
  println(label + "4. Date arithmetic")

  spark.sql(
    """
    |SELECT
    |  datediff(DATE '2025-03-01', DATE '2025-01-10')            AS days_between,
    |  date_add(DATE '2025-01-31', 1)                            AS jan31_plus1,
    |  date_sub(DATE '2025-03-01', 1)                            AS mar1_minus1,
    |  months_between(DATE '2025-03-01', DATE '2025-01-10')      AS months_part,
    |  months_between(DATE '2025-01-31', DATE '2025-02-28')      AS months_feb,
    |  CAST(TIMESTAMP '2025-01-10 10:00:00' AS DATE)             AS ts_to_date
  """.stripMargin).show(false)

  // interval arithmetic, including hours/minutes, via expressions
  orders.select(
    col("order_id"),
    expr("order_ts + INTERVAL 2 HOUR").as("plus_2h"),
    expr("order_ts - INTERVAL 30 MINUTE").as("minus_30m"),
    datediff(date_add(col("order_date"), 30), col("order_date")).as("days_between"),
    months_between(add_months(col("order_date"), 1), col("order_date")).as("months_part"))
    .orderBy("order_id")
    .show(false)

  // rdd
  orders.rdd
    .map(row => {
      val d = row.getAs[java.sql.Date]("order_date")
      (row.getAs[Int]("order_id"),
        java.time.temporal.ChronoUnit.DAYS.between(d.toLocalDate, d.toLocalDate.plusDays(30)),
        java.time.temporal.ChronoUnit.MONTHS.between(d.toLocalDate, d.toLocalDate.plusMonths(1)),
        d.toLocalDate.plusDays(1))
    })
    .collect()
    .sortBy(_._1)
    .foreach(println)

  // dataset
  orders.as[Order]
    .map(o => (o.order_id, o.order_date.toLocalDate.plusDays(30).toString, o.order_date.toLocalDate.plusMonths(1).toString))
    .toDF("order_id", "plus_30d", "plus_1m")
    .orderBy("order_id")
    .show(false)

  // =====================================================================
  // 5. Epoch and time zones — TIMESTAMP has no zone, the *string* does
  // =====================================================================
  println(label + "5. Epoch, time zones, non-determinism")

  spark.sql("SELECT current_timezone()").show(false)
  spark.sql("SELECT current_date() AS d, current_timestamp() AS ts, now() AS now_fn").show(false)

  // now()/current_timestamp are constant-folded: one value for the whole query
  val tsFrame = spark.range(3).select(col("id"), current_timestamp().as("ts"))
  tsFrame.show(false)
  println("current_timestamp values in one projection: " +
    tsFrame.select("ts").distinct().count() + " (constant-folded, not per row)")

  orders.select(
    col("order_id"),
    col("order_ts"),
    (unix_timestamp(col("order_ts"))).as("epoch_s"),
    from_unixtime(unix_timestamp(col("order_ts"))).as("round_trip"),
    col("order_ts").cast("string").as("as_text"),
    // the same instant rendered in two zones
    to_utc_timestamp(col("order_ts"), "Asia/Kolkata").as("in_utc"),
    from_utc_timestamp(to_utc_timestamp(col("order_ts"), "Asia/Kolkata"), "UTC").as("back_to_utc"))
    .orderBy("order_id")
    .show(5, false)

  // session zone changes the rendering, not the stored instant
  spark.conf.set("spark.sql.session.timeZone", "UTC")
  orders.select(col("order_id"), col("order_ts"), col("order_ts").cast("string").as("as_utc")).orderBy("order_id").show(3, false)
  spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
  orders.select(col("order_id"), col("order_ts"), col("order_ts").cast("string").as("as_ist")).orderBy("order_id").show(3, false)

  // rdd / dataset
  orders.rdd
    .map(row => {
      val ts = row.getAs[java.sql.Timestamp]("order_ts")
      (row.getAs[Int]("order_id"), ts.getTime, java.time.Instant.ofEpochMilli(ts.getTime).toString)
    })
    .collect().sortBy(_._1).foreach(println)

  orders.as[Order]
    .map(o => (o.order_id, o.order_date.toLocalDate.atStartOfDay(java.time.ZoneOffset.UTC).toInstant.toEpochMilli))
    .toDF("order_id", "epoch_ms")
    .orderBy("order_id")
    .show(5, false)

  // =====================================================================
  // 6. A calendar spine — reporting days/months with no rows still need to
  //    exist. sequence() + explode + a left join is the standard fill.
  // =====================================================================
  println(label + "6. Calendar spine / gap filling")

  val spine = spark.sql(
    """
    |SELECT explode(sequence(DATE '2025-01-01', DATE '2025-03-31', INTERVAL 1 MONTH)) AS month_start
  """.stripMargin)
  spine.show(false)

  val filled = spine.as("s")
    .join(orders.groupBy(trunc(col("order_date"), "month").as("month_start"))
      .agg(sum("amount").as("amt"), count(lit(1)).as("cnt")), Seq("month_start"), "left")
    .select(col("month_start"), coalesce(col("amt"), lit(0.0)).as("amt"), coalesce(col("cnt"), lit(0)).as("cnt"))
    .withColumn("roll_3m_amt", sum("amt").over(Window.orderBy("month_start").rowsBetween(2, 0)))
    .orderBy("month_start")
  filled.show(false)

  // rdd — build the spine by hand
  val monthStart = java.time.LocalDate.of(2025, 1, 1)
  val months = (0 until 3).map(i => monthStart.plusMonths(i.toLong))
  val amtByMonth: Map[java.sql.Date, Double] =
    orders.rdd.map(row => (row.getAs[java.sql.Date]("order_date"), row.getAs[Double]("amount")))
      .collect().groupBy(_._1).map { case (d, rows) =>
        (java.sql.Date.valueOf(d.toLocalDate.withDayOfMonth(1)), rows.map(_._2).sum)
      }
  months.foreach(m => {
    val key = java.sql.Date.valueOf(m)
    println(f"month=$m%-12s amt=${amtByMonth.getOrElse(key, 0.0)}%8.2f")
  })

  // dataset
  months.map { m =>
    val key = java.sql.Date.valueOf(m)
    (m.toString, amtByMonth.getOrElse(key, 0.0))
  }.toDF("month_start", "amt").orderBy("month_start").show(false)

  // =====================================================================
  // 7. Time windows over rows: a session gap / "is it the same day"
  // =====================================================================
  println(label + "7. Time windows over rows")

  spark.sql(
    """
    |SELECT cust_id, order_date,
    |  datediff(order_date, LAG(order_date) OVER (PARTITION BY cust_id ORDER BY order_date)) AS days_since_prev,
    |  SUM(amount) OVER (PARTITION BY cust_id ORDER BY order_date
    |                    RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW)           AS amt_30d
    |FROM orders
    |WINDOW w AS (PARTITION BY cust_id ORDER BY order_date)
    |ORDER BY cust_id, order_date
  """.stripMargin).show(false)

  // rdd — the same with reduceByKey over a sorted stream per customer
  orders.rdd
    .map(row => (row.getAs[Long]("cust_id"),
      (row.getAs[Int]("order_id"), row.getAs[java.sql.Date]("order_date"), row.getAs[Double]("amount"))))
    .groupByKey()
    .map { case (custId, rows) =>
      val sorted = rows.toSeq.sortBy(_._2.getTime)
      var prev: java.sql.Date = null
      sorted.map { case (id, d, amt) =>
        val gap = if (prev == null) -1L
        else java.time.temporal.ChronoUnit.DAYS.between(prev.toLocalDate, d.toLocalDate)
        prev = d
        (custId, id, gap)
      }
    }
    .collect().flatten.sortBy(x => (x._1, x._2)).foreach(println)

  // dataframe
  orders.select(col("cust_id"), col("order_id"), col("order_date"),
    datediff(col("order_date"), lag(col("order_date"), 1).over(Window.partitionBy("cust_id").orderBy("order_date"))).as("days_since_prev"))
    .orderBy("cust_id", "order_id")
    .show(false)

  // dataset
  orders.as[Order]
    .map(o => (o.cust_id, o.order_id, o.order_date))
    .toDF("cust_id", "order_id", "order_date")
    .withColumn("days_since_prev", datediff(col("order_date"), lag(col("order_date"), 1).over(Window.partitionBy("cust_id").orderBy("order_date"))))
    .orderBy("cust_id", "order_id")
    .show(false)

  // =====================================================================
  // 8. Traps, collected
  // =====================================================================
  println(label + "8. Traps")

  // string comparison is lexicographic, and only agrees with dates because
  // ISO-8601 sorts lexicographically
  spark.sql(
    """
    |SELECT '2025-1-2' < '2025-01-10' AS str_lt,
    |       CAST('2025-1-2' AS DATE)    AS loose_parse,
    |       to_date('2025-1-2', 'yyyy-M-d') AS strict_parse
  """.stripMargin).show(false)

  // non-ISO input is null (legacy.timeParserPolicy=EXCEPTION would throw)
  println("to_date('2025-01-10 10:00:00.123', 'yyyy-MM-dd') -> " +
    spark.sql("SELECT to_date('2025-01-10 10:00:00.123', 'yyyy-MM-dd') AS d").head().get(0))

  // trunc() needs the unit as a *string* for dates, a number for timestamps
  println("trunc(date,'month') -> " + spark.sql("SELECT trunc(DATE '2025-02-17', 'month') AS d").head().get(0))
  println("trunc(ts, 1 hour)   -> " + spark.sql("SELECT trunc(TIMESTAMP '2025-02-17 13:45:59', 1, 'hour') AS t").head().get(0))

  // months_between is fractional and asymmetric
  spark.sql(
    """
    |SELECT months_between(DATE '2025-01-31', DATE '2025-02-01') AS m1,
    |       months_between(DATE '2025-02-01', DATE '2025-01-31') AS m2,
    |       add_months(DATE '2025-01-31', 1)                 AS clamped
  """.stripMargin).show(false)

  spark.stop()
}
