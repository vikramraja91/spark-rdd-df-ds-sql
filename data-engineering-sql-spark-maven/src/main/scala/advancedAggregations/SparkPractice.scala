package advancedAggregations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import datasets._

object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("06-advanced-aggregations")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)
  customerAll.createOrReplaceTempView("customers")
  orders.createOrReplaceTempView("orders")

  /**
   * SUBTOTALs: rollup produces grouped rows plus grand-total rows.
   */
  // sql (temp view)
  spark.sql("""
    SELECT city, COUNT(*) AS cnt
    FROM customers
    GROUP BY ROLLUP(city)
  """).show(false)

  // rdd — manual: aggregate per single-column key, then add the grand total
  val perCityCounts = customerAll.rdd
    .map(row => (row.getAs[String]("city") -> 1))
    .reduceByKey(_ + _)
    .collect()
    .map { case (k, c) => (Some(k), c) }
  perCityCounts.foreach(println)
  val total = perCityCounts.map(_._2).sum
  println((None, total))

  // dataframe
  customerAll.rollup("city").count().show(false)

  // dataset
  customerAll.as[Customer]
    .map(c => c.city)
    .toDF("city")
    .rollup("city")
    .count()
    .show(false)


  /**
   * CUBE: aggregations over every combination of the listed columns.
   */
  // sql (temp view)
  spark.sql("""
    SELECT city, status, COUNT(*) AS cnt
    FROM customers c
    LEFT JOIN orders o ON c.cust_id = o.cust_id
    GROUP BY CUBE(c.city, o.status)
  """).show(false)

  // rdd — manual: group by each combination of the cube keys
  val cubeKeys = customerAll.rdd
    .map(row => (row.getAs[Long]("cust_id"), row.getAs[String]("city")))
    .leftOuterJoin(
      orders.rdd.map(row => (row.getAs[Int]("cust_id").toLong, row.getAs[String]("status")))
    )
  val withBoth   = cubeKeys.map { case (_, (city, st)) => ((city, st.getOrElse("")), 1) }.reduceByKey(_ + _)
  val withCity   = cubeKeys.map { case (_, (city, _)) => ((city, ""), 1) }.reduceByKey(_ + _)
  val withStatus = cubeKeys.map { case (_, (_, st)) => (("", st.getOrElse("")), 1) }.reduceByKey(_ + _)
  val grandTotal = cubeKeys.count().toInt
  (withBoth ++ withCity ++ withStatus ++ spark.sparkContext.parallelize(Seq((("", ""), grandTotal)))).foreach(println)

  // dataframe
  customerAll
    .join(orders, Seq("cust_id"), "left")
    .cube("city", "status")
    .count()
    .show(false)

  // dataset
  customerAll.as[Customer]
    .joinWith(orders.as[Order], customerAll("cust_id") === orders("cust_id"), "left")
    .map { case (c, o) => (c.city, Option(o).map(_.status).orNull) }
    .toDF("city", "status")
    .cube("city", "status")
    .count()
    .show(false)


  /**
   * PIVOT: turn status values into columns with a given aggregation.
   */
  // sql (temp view) — Spark SQL has no native PIVOT; emulated with CASE WHEN
  spark.sql("""
    SELECT cust_id,
           SUM(CASE WHEN status = 'Completed' THEN amount ELSE 0 END) AS completed_amt,
           SUM(CASE WHEN status = 'Pending'   THEN amount ELSE 0 END) AS pending_amt,
           SUM(CASE WHEN status = 'Cancelled' THEN amount ELSE 0 END) AS cancelled_amt
    FROM orders
    GROUP BY cust_id
  """).show(false)

  // rdd — manual: reduce into a per-key Map of status -> total
  orders.rdd
    .map(row => (row.getAs[Int]("cust_id"), Map(row.getAs[String]("status") -> row.getAs[Double]("amount"))))
    .reduceByKey((m1, m2) => m1 ++ m2.map { case (k, v) => k -> (v + m1.getOrElse(k, 0.0)) })
    .foreach(println)

  // dataframe
  orders
    .groupBy("cust_id")
    .pivot("status", Seq("Completed", "Pending", "Cancelled"))
    .agg(sum("amount").alias("amt"))
    .show(false)

  // dataset
  orders.as[Order]
    .groupBy(_.cust_id)
    .aggregateByKey(Map.empty[String, Double])(
      (acc, o) => acc + (o.status -> (o.amount + acc.getOrElse(o.status, 0.0))),
      (a, b) => a ++ b.map { case (k, v) => k -> (v + a.getOrElse(k, 0.0)) }
    )
    .show(false)


  /**
   * collect_list / collect_set: gather column values into arrays.
   */
  // sql (temp view)
  spark.sql("""
    SELECT cust_id,
           COLLECT_LIST(status) AS all_statuses,
           COLLECT_SET(status)  AS distinct_statuses
    FROM orders
    GROUP BY cust_id
  """).show(false)

  // rdd
  orders.rdd
    .map(row => (row.getAs[Int]("cust_id"), row.getAs[String]("status")))
    .groupByKey()
    .mapValues(statuses => (statuses.toList, statuses.toSet))
    .foreach(println)

  // dataframe
  orders.groupBy("cust_id")
    .agg(collect_list("status").alias("all_statuses"), collect_set("status").alias("distinct_statuses"))
    .show(false)

  // dataset
  orders.as[Order]
    .groupByKey(_.cust_id)
    .agg(collect_list("status").alias("all_statuses"), collect_set("status").alias("distinct_statuses"))
    .show(false)


  /**
   * Approximate aggregations for large data: approx_count_distinct.
   */
  // sql (temp view)
  spark.sql("SELECT approx_count_distinct(cust_id) AS approx_distinct FROM orders").show(false)

  // rdd — exact (no built-in approximate distinct in RDD API)
  orders.rdd.map(row => row.getAs[Int]("cust_id")).distinct().count()

  // dataframe
  orders.select(approx_count_distinct("cust_id").alias("approx_distinct")).show(false)

  // dataset
  orders.as[Order].select(approx_count_distinct("cust_id").alias("approx_distinct")).show(false)

  spark.stop()
}