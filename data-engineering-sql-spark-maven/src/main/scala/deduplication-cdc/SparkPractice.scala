package deduplicationCdc

import datasets.{CustomerUpdate, PracticeData}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
 * 16 - Change data capture: deduplication, last-write-wins, SCD2 history,
 * and a portable upsert.
 *
 * Three things to take away:
 *   1. `dropDuplicates` / `dropDuplicatesWithinWatermark` pick an *arbitrary*
 *      row per key. That is only correct when the remaining columns are
 *      identical, or when `spark.sql.shuffle.partitions` and the input order
 *      happen to cooperate. Deterministic de-dup needs a window function.
 *   2. A CDC stream re-delivers the same change, so every batch must be
 *      idempotent: dedup on the *business* key with a version/ts tie-break.
 *   3. `MERGE INTO` is not implemented in vanilla Spark 3.5, so an upsert is
 *      an anti-join + union (or an overwrite with a filter condition).
 */
object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("16-deduplication-cdc")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.warehouse.dir", "target/warehouse-dedup-cdc")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)
  customerAll.createOrReplaceTempView("customers")
  orders.createOrReplaceTempView("orders")

  val label = "\n---------- "

  // The CDC stream. PracticeData gives 6 records for 3 customers; the same
  // record_id is re-delivered below to imitate an at-least-once source.
  val updatesRaw: DataFrame = customerUpdates
  val updates1: DataFrame = customerUpdates
    .withColumn("cust_id", col("cust_id").cast("long"))
    .persist(StorageLevel.MEMORY_AND_DISK)
  updates1.count()
  updates1.createOrReplaceTempView("cdc_1")

  // a re-delivery: the same 6 rows again, plus one brand new record
  val updates2: DataFrame = updates1.unionByName(spark.sql(
    """
    |SELECT 7 AS record_id, 3 AS cust_id, 'Priya' AS cust_name, 'Hyderabad' AS city,
    |       CAST('2025-04-01 09:00:00' AS TIMESTAMP) AS updated_at,
    |       'UPDATE' AS operation
    |FROM Range(1)
  """.stripMargin))
  updates2.createOrReplaceTempView("cdc_2")

  // =====================================================================
  // 1. dropDuplicates — fine only when the duplicates are identical
  // =====================================================================
  println(label + "1. dropDuplicates vs a deterministic window")

  spark.sql("SELECT COUNT(*) FROM cdc_1").show(false)
  spark.sql("SELECT COUNT(*) FROM (SELECT DISTINCT record_id, cust_id, cust_name, city, updated_at, operation FROM cdc_1)").show(false)

  updates1.select("record_id", "cust_id", "cust_name", "city", "updated_at", "operation")
    .dropDuplicates("record_id")
    .orderBy("record_id")
    .show(false)

  // a *partial* dedup: any row per record_id, no control over which
  updates1.dropDuplicates("record_id").count()

  // rdd — distinct() on the whole tuple
  updates1.rdd.map(row => (
      row.getAs[Int]("record_id"), row.getAs[Long]("cust_id"),
      row.getAs[String]("cust_name"), row.getAs[String]("city"),
      row.getAs[java.sql.Timestamp]("updated_at"), row.getAs[String]("operation")))
    .distinct()
    .collect()
    .sortBy(_._1)
    .foreach(println)

  // dataframe — distinct() is the relational spelling
  updates1.select("record_id", "cust_id", "cust_name", "city", "updated_at", "operation")
    .distinct()
    .orderBy("record_id")
    .show(false)

  // dataset
  updatesRaw.as[CustomerUpdate]
    .map(u => (u.record_id, u.cust_id, u.cust_name, u.city, u.updated_at, u.operation))
    .distinct()
    .toDF("record_id", "cust_id", "cust_name", "city", "updated_at", "operation")
    .orderBy("record_id")
    .show(false)

  // =====================================================================
  // 2. Deterministic dedup: row_number() over the version/timestamp
  // =====================================================================
  println(label + "2. Deterministic dedup with row_number")

  val deduped = updates1
    .withColumn("rn", row_number().over(Window.partitionBy("record_id").orderBy(desc("updated_at"))))
    .filter(col("rn") === 1)
    .drop("rn")
  deduped.orderBy("record_id").show(false)

  // rdd — groupByKey + reduce by timestamp (a typed reduce, no TypedColumn)
  deduped.rdd
    .map(row => (row.getAs[Int]("record_id"), row))
    .groupByKey()
    .map { case (_, rows) => rows.reduce((a, b) =>
      if (a.getAs[java.sql.Timestamp]("updated_at").after(b.getAs[java.sql.Timestamp]("updated_at"))) a else b) }
    .map(row => (row.getAs[Int]("record_id"), row.getAs[Long]("cust_id"), row.getAs[String]("city")))
    .collect()
    .sortBy(_._1)
    .foreach(println)

  // dataset
  updatesRaw.as[CustomerUpdate]
    .groupByKey(_.record_id)
    .reduceGroups((a: CustomerUpdate, b: CustomerUpdate) => if (a.updated_at.after(b.updated_at)) a else b)
    .map(_._2)
    .map(u => (u.record_id, u.cust_id, u.cust_name, u.city, u.updated_at, u.operation))
    .toDF("record_id", "cust_id", "cust_name", "city", "updated_at", "operation")
    .orderBy("record_id")
    .show(false)

  // =====================================================================
  // 3. Last-write-wins on the business key (the usual CDC requirement)
  //    Two rows for cust_id 1 with 2025-01-01 and 2025-03-01 -> keep March.
  // =====================================================================
  println(label + "3. Last-write-wins per business key")

  spark.sql(
    """
    |SELECT cust_id, cust_name, city, updated_at, operation
    |FROM (
    |  SELECT *, ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY updated_at DESC, record_id DESC) AS rn
    |  FROM cdc_1
    |) t
    |WHERE rn = 1
    |ORDER BY cust_id
  """.stripMargin).show(false)

  // spark.sql.optimizer.excludedRules can be used to *keep* the Subquery in the
  // plan; the take-away is that the window+filter is the portable form
  val lww = updates1
    .withColumn("rn", row_number().over(Window.partitionBy("cust_id").orderBy(desc("updated_at"), desc("record_id"))))
    .filter(col("rn") === 1)
    .drop("rn")
  lww.orderBy("cust_id").show(false)

  // rdd — reduceByKey needs a "keep the newer" merge
  val lwwRdd = updates1.rdd
    .map(row => (row.getAs[Long]("cust_id"), row))
    .reduceByKey((a, b) =>
      if (a.getAs[java.sql.Timestamp]("updated_at").after(b.getAs[java.sql.Timestamp]("updated_at"))) a else b)
  lwwRdd.map { case (custId, row) =>
    (custId, row.getAs[String]("city"), row.getAs[java.sql.Timestamp]("updated_at"))
  }.collect().sortBy(_._1).foreach(println)

  // dataset
  updatesRaw.as[CustomerUpdate]
    .groupByKey(_.cust_id)
    .reduceGroups((a: CustomerUpdate, b: CustomerUpdate) => if (a.updated_at.after(b.updated_at)) a else b)
    .map(_._2)
    .map(u => (u.cust_id, u.cust_name, u.city, u.updated_at, u.operation))
    .toDF("cust_id", "cust_name", "city", "updated_at", "operation")
    .orderBy("cust_id")
    .show(false)

  // =====================================================================
  // 4. Idempotency check: run batch 2 (re-delivery + 1 new record) and
  //    prove the result is unchanged for the customers it already covered.
  // =====================================================================
  println(label + "4. Idempotency across re-delivered batches")

  val lww1 = lww.select("cust_id", "cust_name", "city", "updated_at", "operation")
  val lww2 = updates2
    .withColumn("rn", row_number().over(Window.partitionBy("cust_id").orderBy(desc("updated_at"), desc("record_id"))))
    .filter(col("rn") === 1)
    .drop("rn")
    .select("cust_id", "cust_name", "city", "updated_at", "operation")

  lww2.orderBy("cust_id").show(false)

  // the merge of the two batches, done the portable way: full outer join
  // (explicit condition, so both cust_id columns survive) and COALESCE
  val merged = lww1.as("a").join(lww2.as("b"), col("a.cust_id") === col("b.cust_id"), "full_outer")
    .select(
      coalesce(col("a.cust_id"), col("b.cust_id")).as("cust_id"),
      coalesce(col("a.cust_name"), col("b.cust_name")).as("cust_name"),
      coalesce(col("a.city"), col("b.city")).as("city"),
      greatest(col("a.updated_at"), col("b.updated_at")).as("updated_at"),
      coalesce(col("a.operation"), col("b.operation")).as("operation"))
  merged.orderBy("cust_id").show(false)

  // a full outer join is the reconciliation pattern: rows only in "a" are
  // missing from the new batch, rows only in "b" are new
  println("only in batch 1: " + lww1.as("a").join(lww2.as("b"), col("a.cust_id") === col("b.cust_id"), "left_anti").count())
  println("only in batch 2: " + lww2.as("b").join(lww1.as("a"), col("a.cust_id") === col("b.cust_id"), "left_anti").count())

  // =====================================================================
  // 5. SCD Type 1 — overwrite in place (history is not kept)
  // =====================================================================
  println(label + "5. SCD Type 1 (current-state only)")

  val scd1 = customerAll.as("c")
    .join(lww.as("u"), Seq("cust_id"), "left")
    .select(
      col("c.cust_id"),
      coalesce(col("u.cust_name"), col("c.cust_name")).as("cust_name"),
      coalesce(col("u.city"), col("c.city")).as("city"),
      col("c.signup_date"))
  scd1.filter(col("cust_id") <= 5).orderBy("cust_id").show(false)

  // =====================================================================
  // 6. SCD Type 2 — one row per change, closed with valid_to_date
  //    For each customer, every CDC row becomes a version; the previous
  //    version's valid_to_date is the next version's valid_from - 1 day.
  // =====================================================================
  println(label + "6. SCD Type 2 (history with validity window)")

  val history = updates1
    .withColumn("valid_from", col("updated_at"))
    .withColumn("valid_to", lead("updated_at", 1).over(Window.partitionBy("cust_id").orderBy(col("updated_at"))))
  val scd2 = history.select(
    col("cust_id"),
    col("cust_name"),
    col("city"),
    col("updated_at").as("valid_from"),
    coalesce(date_sub(col("valid_to").cast("date"), 1), lit("9999-12-31").cast("date")).as("valid_to"),
    col("operation"))
  scd2.filter(col("cust_id") <= 3).orderBy("cust_id", "valid_from").show(false)

  // rdd — the rows are already (cust_id, ...) keyed, so stitch the windows with
  // a groupBy on the driver side of a collect()
  def scd2Rdd(input: org.apache.spark.rdd.RDD[((Long, String, String, java.sql.Timestamp), Int)])
    : Seq[(Long, String, String, java.sql.Timestamp, java.sql.Timestamp, Boolean)] =
    input.collect()
      .groupBy(_._1._1)
      .toSeq
      .sortBy(_._1)
      .flatMap { case (custId, rows) =>
        val sorted = rows.sortBy(r => r._1._4.getTime)
        sorted.zipWithIndex.map { case ((row, _), idx) =>
          val nextTs = sorted.lift(idx + 1).map(_._1._4)
          (custId, row._2, row._3, row._4, nextTs.getOrElse(new java.sql.Timestamp(4102444800000L)), nextTs.isEmpty)
        }
      }

  scd2Rdd(updates1.rdd.map(row => (
      (row.getAs[Long]("cust_id"), row.getAs[String]("cust_name"),
        row.getAs[String]("city"), row.getAs[java.sql.Timestamp]("updated_at")), row.getAs[Int]("record_id"))))
    .filter(_._1 <= 3)
    .foreach(x => println(f"cust=${x._1}%-4d ${x._2}%-8s ${x._3}%-10s from=${x._4} to=${x._5} current=${x._6}"))

  // dataset — the typed read of the same history. The window is built with
  // the column API (Dataset.withColumn), then bound back to a case class: the
  // typed API has no way to express LEAD() over a grouped Dataset.
  case class Scd2Row(cust_id: Long, cust_name: String, city: String,
                      valid_from: java.sql.Timestamp, valid_to: java.sql.Timestamp, operation: String)

  scd2.as[Scd2Row]
    .filter(_.cust_id <= 3)
    .map(r => (r.cust_id, r.cust_name, r.city, r.valid_from, r.valid_to, r.operation))
    .toDF("cust_id", "cust_name", "city", "valid_from", "valid_to", "operation")
    .orderBy("cust_id", "valid_from")
    .show(false)

  // =====================================================================
  // 7. Upsert without MERGE: anti-join + union (the portable pattern)
  // =====================================================================
  println(label + "7. Upsert with anti-join + union (MERGE is not implemented)")

  val target = customerAll.filter(col("cust_id") <= 3).select("cust_id", "cust_name", "city", "signup_date")

  // the incoming batch touches customer 2 (an update) and adds customer 4
  // (an insert), so the upsert has to take both branches
  val newCustomer = spark.sql(
    """
    |SELECT 4L AS cust_id, 'NewCo' AS cust_name, 'Pune' AS city,
    |       CAST('2025-05-01 08:00:00' AS TIMESTAMP) AS updated_at, 'INSERT' AS operation
    |FROM range(1)
  """.stripMargin)
  val incomingRaw = lww2.select("cust_id", "cust_name", "city", "updated_at", "operation")
    .unionByName(newCustomer)
    .filter(col("cust_id") === 2L || col("cust_id") === 4L)
  // an incoming row carries no signup_date unless it is an INSERT; the date it
  // was inserted is the honest substitute
  val incoming = incomingRaw.select(
    col("cust_id"),
    col("cust_name"),
    col("city"),
    when(col("operation") === "INSERT", col("updated_at").cast("date"))
      .otherwise(lit(null).cast("date")).as("signup_date"))

  // MERGE INTO is rejected by the vanilla parser — show the real error
  try {
    spark.sql(
      """
      |MERGE INTO TABLE customers t
      |USING cdc_1 s
      |ON t.cust_id = s.cust_id
      |WHEN MATCHED THEN UPDATE SET t.city = s.city
      |WHEN NOT MATCHED THEN INSERT (cust_id, cust_name, city) VALUES (s.cust_id, s.cust_name, s.city)
    """.stripMargin)
  } catch {
    case e: Throwable =>
      val msg = e.getMessage.linesIterator.map(_.trim).find(_.nonEmpty).getOrElse("")
      println("MERGE INTO -> " + e.getClass.getSimpleName + ": " + msg)
  }

  val unchanged = target.join(incoming.as("i"), Seq("cust_id"), "left_anti")
  val inserted = incoming.join(target.as("t"), Seq("cust_id"), "left_anti")
  val upserted = unchanged.select("cust_id", "cust_name", "city", "signup_date")
    .unionByName(inserted.select("cust_id", "cust_name", "city", "signup_date"))
  println("rows kept (no change): " + unchanged.count() + ", rows inserted/updated: " + inserted.count())
  upserted.orderBy("cust_id").show(false)

  // and the write path: an overwrite of only the affected partitions
  val affected = incoming.select("cust_id").distinct()
  val replacement = target.join(affected, Seq("cust_id"), "left_anti")
    .select("cust_id", "cust_name", "city", "signup_date")
    .unionByName(inserted.select("cust_id", "cust_name", "city", "signup_date"))
  println("if writing back to a partitioned table, only these cust_ids are rewritten: " +
    replacement.select("cust_id").orderBy("cust_id").collect().map(_.getLong(0)).mkString(","))

  // =====================================================================
  // 8. Out-of-order / late arrivals: a watermark comparison
  //    If a batch can carry an older updated_at than what is already applied,
  //    last-write-wins silently drops it. Count them instead of hiding them.
  // =====================================================================
  println(label + "8. Late-arriving records")

  // batch 1 is the "applied" state; every record of the re-delivered batch 2
  // that is older than it is one last-write-wins would silently drop
  val appliedMax = lww1.groupBy("cust_id").agg(max("updated_at").as("applied_ts"))
  val late = updates2.join(appliedMax, Seq("cust_id"), "left")
    .filter(col("updated_at") < col("applied_ts"))
    .select("record_id", "cust_id", "updated_at", "applied_ts")
    .orderBy("cust_id", "updated_at")
  println("records older than the applied state (LWW drops these): " + late.count())
  late.show(false)

  updates1.unpersist()
  spark.stop()
}
