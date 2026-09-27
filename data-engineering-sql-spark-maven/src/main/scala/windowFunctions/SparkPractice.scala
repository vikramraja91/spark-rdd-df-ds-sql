package windowFunctions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import datasets._

/**
 * 05 - Window functions: row_number, rank, dense_rank, lead/lag,
 * running totals and cumulative aggregates.
 *
 * Note: there is no native typed window API for Datasets — window functions
 * are applied through the DataFrame API (withColumn), so the "dataset"
 * section below works on `customers.as[Customer]` but calls the column API.
 */
object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("05-window-functions")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)
  customerAll.createOrReplaceTempView("customers")
  orders.createOrReplaceTempView("orders")

  val ordersCustDs = orders.as[Order]
  val ordersRdd = orders.rdd
    .map(row => (row.getAs[Int]("cust_id"), (row.getAs[Int]("order_id"), row.getAs[java.sql.Date]("order_date"), row.getAs[Double]("amount"), row.getAs[String]("status"))))

  /**
   * Rank orders within each customer by amount (descending).
   */
  val byCust = Window.partitionBy("cust_id").orderBy(col("amount").desc)

  // sql (temp view)
  spark.sql("""
    SELECT cust_id, order_id, amount,
           ROW_NUMBER()   OVER (PARTITION BY cust_id ORDER BY amount DESC) AS rn,
           RANK()         OVER (PARTITION BY cust_id ORDER BY amount DESC) AS rk,
           DENSE_RANK()   OVER (PARTITION BY cust_id ORDER BY amount DESC) AS drk
    FROM orders
  """).show(false)

  // rdd — manual: group by cust_id, sort, then zipWithIndex
  ordersRdd
    .groupByKey()
    .flatMapValues(rows => rows.toSeq.sortBy(-_._3).zipWithIndex.map { case ((oid, dt, amt, st), i) => (oid, amt, i + 1) })
    .foreach(println)

  // dataframe
  orders
    .withColumn("rn", row_number().over(byCust))
    .withColumn("rk", rank().over(byCust))
    .withColumn("drk", dense_rank().over(byCust))
    .show(false)

  // dataset — via the DataFrame/column API
  ordersCustDs
    .withColumn("rn", row_number().over(byCust))
    .withColumn("rk", rank().over(byCust))
    .withColumn("drk", dense_rank().over(byCust))
    .show(false)


  /**
   * Compare each order with the customer's previous order (lag) and
   * next order (lead) by amount.
   */
  val byCustByDate = Window.partitionBy("cust_id").orderBy(col("order_date"))

  // sql (temp view)
  spark.sql("""
    SELECT cust_id, order_id, order_date, amount,
           LAG(amount, 1)  OVER (PARTITION BY cust_id ORDER BY order_date)  AS prev_amount,
           LEAD(amount, 1) OVER (PARTITION BY cust_id ORDER BY order_date)  AS next_amount
    FROM orders
  """).show(false)

  // rdd — manual: pair each row with the previous row in the sorted group
  ordersRdd
    .groupByKey()
    .flatMap { case (custId, rows) =>
      val sorted = rows.toSeq.sortBy(_._2.getTime)
      val prevs: Seq[Option[Double]] = None +: sorted.dropRight(1).map(r => Option(r._3))
      sorted.zip(prevs).map { case (cur, prev) => (custId, cur._1, cur._3, prev) }
    }
    .foreach(println)

  // dataframe
  orders
    .withColumn("prev_amount", lag("amount", 1).over(byCustByDate))
    .withColumn("next_amount", lead("amount", 1).over(byCustByDate))
    .show(false)

  // dataset
  ordersCustDs
    .withColumn("prev_amount", lag("amount", 1).over(byCustByDate))
    .withColumn("next_amount", lead("amount", 1).over(byCustByDate))
    .show(false)


  /**
   * Running total of amount per customer (cumulative sum ordered by date).
   */
  val runningByDate = Window.partitionBy("cust_id").orderBy(col("order_date")).rowsBetween(Window.unboundedPreceding, Window.currentRow)

  // sql (temp view)
  spark.sql("""
    SELECT cust_id, order_id, order_date, amount,
           SUM(amount) OVER (PARTITION BY cust_id ORDER BY order_date
                             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
    FROM orders
  """).show(false)

  // rdd — manual: scan prefix sums
  ordersRdd
    .groupByKey()
    .flatMap { case (custId, rows) =>
      val sorted = rows.toSeq.sortBy(_._2.getTime)
      val (res, _) = sorted.foldLeft((List.empty[(Int, Double)], 0.0)) { case ((acc, sum), (oid, _, amt, _)) =>
        (acc :+ (oid, sum + amt), sum + amt)
      }
      res.map { case (oid, total) => (custId, oid, total) }
    }
    .foreach(println)

  // dataframe
  orders
    .withColumn("running_total", sum("amount").over(runningByDate))
    .show(false)

  // dataset
  ordersCustDs
    .withColumn("running_total", sum("amount").over(runningByDate))
    .show(false)


  /**
   * Keep the highest-spending order per customer (dedupe by row_number).
   */
  // sql (temp view)
  spark.sql("""
    SELECT cust_id, order_id, amount
    FROM (
      SELECT cust_id, order_id, amount,
             ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY amount DESC) AS rn
      FROM orders
    ) t
    WHERE t.rn = 1
  """).show(false)

  // rdd — manual: pick the max within each group
  ordersRdd
    .groupByKey()
    .mapValues(rows => rows.toSeq.maxBy(_._3))
    .map { case (custId, (oid, _, amt, _)) => (custId, oid, amt) }
    .foreach(println)

  // dataframe
  orders
    .withColumn("rn", row_number().over(byCust))
    .filter(col("rn") === 1)
    .select("cust_id", "order_id", "amount")
    .show(false)

  // dataset
  ordersCustDs
    .withColumn("rn", row_number().over(byCust))
    .filter(col("rn") === 1)
    .select("cust_id", "order_id", "amount")
    .show(false)

  spark.stop()
}