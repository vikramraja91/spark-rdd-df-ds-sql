package aggregations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import datasets._

object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("03-aggregations")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)
  customerAll.createOrReplaceTempView("customers")
  orders.createOrReplaceTempView("orders")

  /**
   * Count all customers.
   */
  // sql (temp view)
  spark.sql("SELECT COUNT(*) FROM customers").show(false)

  // rdd
  customerAll.rdd.count()

  // dataframe
  customerAll.count()

  // dataset
  customerAll.as[Customer].count()


  /**
   * Count customers in Hyderabad.
   */
  // sql (temp view)
  spark.sql("SELECT COUNT(*) FROM customers WHERE city = 'hyd'").show(false)

  // rdd
  customerAll.rdd.filter(row => row.getAs[String]("city") == "hyd").count()

  // dataframe
  customerAll.filter(col("city") === "hyd").count()

  // dataset
  customerAll.as[Customer].filter(_.city == "hyd").count()


  /**
   * Count customers by city.
   */
  // sql (temp view)
  spark.sql("SELECT city, COUNT(*) AS customer_count FROM customers GROUP BY city").show(false)

  // rdd
  customerAll.rdd
    .map(row => (row.getAs[String]("city"), 1))
    .reduceByKey(_ + _)
    .foreach(println)

  // dataframe
  customerAll.groupBy("city").count().show(false)

  // dataset
  customerAll.as[Customer]
    .groupByKey(_.city)
    .count()
    .show(false)


  /**
   * Show cities having more than 100 customers.
   */
  // sql (temp view)
  spark.sql("SELECT city, COUNT(*) AS customer_count FROM customers GROUP BY city HAVING COUNT(*) > 100").show(false)

  // rdd
  customerAll.rdd
    .map(row => (row.getAs[String]("city"), 1))
    .reduceByKey(_ + _)
    .filter { case (_, cnt) => cnt > 100 }
    .foreach(println)

  // dataframe
  customerAll.groupBy("city").count().filter(col("count") > 100).show(false)

  // dataset
  customerAll.as[Customer]
    .groupByKey(_.city)
    .count()
    .filter(pair => pair._2 > 100)
    .show(false)


  /**
   * Calculate total order amount by customer.
   */
  // sql (temp view)
  spark.sql("SELECT cust_id, SUM(amount) AS total_spending FROM orders GROUP BY cust_id").show(false)

  // rdd
  orders.rdd
    .map(row => (row.getAs[Int]("cust_id"), row.getAs[Double]("amount")))
    .reduceByKey(_ + _)
    .foreach(println)

  // dataframe
  orders.groupBy("cust_id").agg(sum("amount").alias("total_spending")).show(false)

  // dataset
  orders.as[Order]
    .groupByKey(_.cust_id)
    .reduceGroups((order1, order2) => order1.copy(amount = order1.amount + order2.amount))
   .show(false)


  /**
   * Calculate average order amount by status.
   */
  // sql (temp view)
  spark.sql("SELECT status, AVG(amount) AS avg_amount FROM orders GROUP BY status").show(false)

  // rdd
  orders.rdd
    .map(row => (row.getAs[String]("status"), (row.getAs[Double]("amount"), 1)))
    .reduceByKey { case ((sum1, cnt1), (sum2, cnt2)) => (sum1 + sum2, cnt1 + cnt2) }
    .mapValues { case (sum, cnt) => sum / cnt }
    .foreach(println)

  // dataframe
  orders.groupBy("status").agg(avg("amount").alias("avg_amount")).show(false)

  // dataset
  orders.as[Order]
    .groupByKey(_.status)
    .mapGroups { case (status, rows) =>
      val values = rows.map(_.amount).toSeq
      (status, values.sum / values.size)
    }
    .show(false)

  spark.stop()
}
