package joins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import datasets._

object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("04-joins")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)
  customerAll.createOrReplaceTempView("customers")
  orders.createOrReplaceTempView("orders")

  /**
   * Return customer name and order details for matching customers (inner join).
   */
  // sql (temp view)
  spark.sql("""
    SELECT c.cust_id, c.cust_name, o.order_id, o.amount, o.status
    FROM customers c
    INNER JOIN orders o ON c.cust_id = o.cust_id
  """).show(false)

  // rdd
  val customerPairs = customerAll.rdd.map(row => (row.getAs[Long]("cust_id"), row.getAs[String]("cust_name")))
  val orderPairs = orders.rdd.map(row =>
    (row.getAs[Int]("cust_id").toLong, (row.getAs[Int]("order_id"), row.getAs[Double]("amount"), row.getAs[String]("status")))
  )
  customerPairs.join(orderPairs).foreach(println)

  // dataframe
  customerAll
    .join(orders, Seq("cust_id"), "inner")
    .select("cust_id", "cust_name", "order_id", "amount", "status")
    .show(false)

  // dataset
  customerAll.as[Customer]
    .joinWith(orders.as[Order], customerAll("cust_id") === orders("cust_id"), "inner")
    .show(false)


  /**
   * Return all customers and their orders, including customers without orders (left join).
   */
  // sql (temp view)
  spark.sql("""
    SELECT c.cust_id, c.cust_name, o.order_id, o.amount
    FROM customers c
    LEFT JOIN orders o ON c.cust_id = o.cust_id
  """).show(false)

  // rdd
  customerPairs.leftOuterJoin(orderPairs).foreach(println)

  // dataframe
  customerAll.join(orders, Seq("cust_id"), "left").show(false)

  // dataset
  customerAll.as[Customer]
    .joinWith(orders.as[Order], customerAll("cust_id") === orders("cust_id"), "left")
    .show(false)


  /**
   * Return customers who have at least one order.
   */
  // sql (temp view)
  spark.sql("""
    SELECT DISTINCT c.cust_id, c.cust_name
    FROM customers c
    INNER JOIN orders o ON c.cust_id = o.cust_id
  """).show(false)

  // rdd
  customerPairs.join(orderPairs).map { case (custId, (custName, _)) => (custId, custName) }.distinct().foreach(println)

  // dataframe
  customerAll.join(orders, Seq("cust_id"), "inner").select("cust_id", "cust_name").distinct().show(false)

  // dataset
  customerAll.as[Customer]
    .joinWith(orders.as[Order], customerAll("cust_id") === orders("cust_id"), "inner")
    .map { case (c, _) => (c.cust_id, c.cust_name) }
    .distinct()
    .show(false)


  /**
   * Customers whose total spending is greater than 1000.
   */
  // sql (temp view)
  spark.sql("""
    SELECT c.cust_id, c.cust_name, SUM(o.amount) AS total_spending
    FROM customers c
    INNER JOIN orders o ON c.cust_id = o.cust_id
    GROUP BY c.cust_id, c.cust_name
    HAVING SUM(o.amount) > 1000
  """).show(false)

  // rdd
  customerPairs.join(orderPairs)
    .map { case (custId, (custName, (_, amount, _))) => ((custId, custName), amount) }
    .reduceByKey(_ + _)
    .filter { case (_, totalSpending) => totalSpending > 1000 }
    .foreach(println)

  // dataframe
  customerAll
    .join(orders, Seq("cust_id"), "inner")
    .groupBy("cust_id", "cust_name")
    .agg(sum("amount").alias("total_spending"))
    .filter(col("total_spending") > 1000)
    .show(false)

  // dataset
  customerAll.as[Customer]
    .joinWith(orders.as[Order], customerAll("cust_id") === orders("cust_id"), "inner")
    .groupByKey { case (c, _) => (c.cust_id, c.cust_name) }
    .mapGroups { case (key, rows) => (key._1, key._2, rows.map(_._2.amount).sum) }
    .filter(_._3 > 1000)
    .show(false)


  /**
   * Return customers with no orders.
   */
  // sql (temp view)
  spark.sql("""
    SELECT c.*
    FROM customers c
    LEFT JOIN orders o ON c.cust_id = o.cust_id
    WHERE o.order_id IS NULL
  """).show(false)

  // rdd
  customerAll.rdd
    .map(row => (row.getAs[Long]("cust_id"), row))
    .subtractByKey(orderPairs)
    .map(_._2)
    .foreach(println)

  // dataframe
  customerAll.join(orders, Seq("cust_id"), "left_anti").show(false)

  // dataset
  customerAll.as[Customer]
    .joinWith(orders.as[Order], customerAll("cust_id") === orders("cust_id"), "left_anti")
    .show(false)


  /**
   * Return total spending per customer including customers with no orders.
   */
  // sql (temp view)
  spark.sql("""
    SELECT c.cust_id, c.cust_name, COALESCE(SUM(o.amount), 0) AS total_spending
    FROM customers c
    LEFT JOIN orders o ON c.cust_id = o.cust_id
    GROUP BY c.cust_id, c.cust_name
  """).show(false)

  // rdd
  customerPairs.leftOuterJoin(orderPairs)
    .map { case (custId, (custName, orderOpt)) => ((custId, custName), orderOpt.map(_._2).getOrElse(0.0)) }
    .reduceByKey(_ + _)
    .foreach(println)

  // dataframe
  customerAll
    .join(orders, Seq("cust_id"), "left")
    .groupBy("cust_id", "cust_name")
    .agg(coalesce(sum("amount"), lit(0)).alias("total_spending"))
    .show(false)

  // dataset
  customerAll.as[Customer]
    .joinWith(orders.as[Order], customerAll("cust_id") === orders("cust_id"), "left")
    .groupByKey { case (c, _) => (c.cust_id, c.cust_name) }
    .mapGroups { case (key, rows) =>
      val total = rows.map(r => Option(r._2).map(_.amount).getOrElse(0.0)).sum
      (key._1, key._2, total)
    }
    .show(false)

  spark.stop()
}
