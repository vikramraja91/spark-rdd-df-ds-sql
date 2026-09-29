package subqueries

import datasets.PracticeData
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object SparkPractice extends App {

  val spark = SparkSession
    .builder()
    .appName("15-subqueries")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.warehouse.dir", "target/warehouse-subqueries")
    .getOrCreate()

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)
  val customers = customerAll
  customerAll.createOrReplaceTempView("customers")
  orders.createOrReplaceTempView("orders")
  customerUpdates.createOrReplaceTempView("customerUpdates")

  // Customer.cust_id is a Long, Order.cust_id an Int. SQL comparison widens
  // them implicitly, but a DataFrame join on Seq("cust_id") demands equal
  // types — so joins to the customers side use the cast copy.
  val ordersL = orders.withColumn("cust_id", col("cust_id").cast("long"))
  // the same cast, exposed as a view: a *correlated* subquery is decorrelated
  // into a join, and the placeholder attribute must match the outer column's
  // type or analysis fails with "key not found: cust_id#64"
  ordersL.createOrReplaceTempView("orders_c")

  def label: String = "\n---------- "

  // =====================================================================
  // 1. Uncorrelated scalar subquery — one value, recomputed per block
  //    (the planner folds the whole thing into a single global aggregate).
  // =====================================================================
  println(label + "1. Uncorrelated scalar subquery")

  spark.sql(
    """
    |SELECT cust_id, amount,
    |       (SELECT ROUND(AVG(amount), 2) FROM orders) AS avg_order_amt,
    |       (SELECT COUNT(*)        FROM orders)       AS order_cnt
    |FROM orders
    |WHERE cust_id = 1
    |ORDER BY order_id
  """.stripMargin).show(false)

  // compute the "global" values once, on the driver — an action inside a
  // task closure would try to reach the SparkSession from an executor
  val rddAvg = orders.rdd.map(_.getAs[Double]("amount")).mean()
  val orderCount = orders.count()
  val rddOrders = orders.rdd
    .filter(_.getAs[Int]("cust_id") == 1)
    .map(row => (row.getAs[Int]("order_id"), row.getAs[Double]("amount"), rddAvg, orderCount))
    .collect()
  rddOrders.foreach(println)

  // dataframe — the subquery is just a scalar you computed
  orders.filter(col("cust_id") === 1)
    .withColumn("avg_order_amt", lit(rddAvg))
    .withColumn("order_cnt", lit(orderCount))
    .show(false)

  // dataset — a typed map needs the scalar as a captured value
  orders.as[datasets.Order]
    .filter(_.cust_id == 1)
    .map(o => (o.order_id, o.amount, rddAvg, orderCount))
    .show(false)

  // =====================================================================
  // 2. Scalar subquery in WHERE — "bigger than the average" per row
  // =====================================================================
  println(label + "2. Scalar subquery in WHERE")

  spark.sql(
    """
    |SELECT order_id, cust_id, amount
    |FROM orders
    |WHERE amount > (SELECT AVG(amount) FROM orders)
    |ORDER BY amount DESC
  """.stripMargin).show(false)

  // rdd — a broadcast of the single aggregate value, then filter
  val globalAvg = orders.rdd.map(_.getAs[Double]("amount")).mean()
  orders.rdd
    .filter(_.getAs[Double]("amount") > globalAvg)
    .map(row => (row.getAs[Int]("order_id"), row.getAs[Double]("amount")))
    .collect()
    .sortBy(-_._2)
    .foreach(println)

  // dataframe
  orders.filter(col("amount") > globalAvg).show(false)

  // dataset
  orders.as[datasets.Order].filter(_.amount > globalAvg).show(false)

  // =====================================================================
  // 3. IN / NOT IN — and the NULL trap
  //    A NULL in the IN-list makes "NOT IN" return NO rows (three-valued
  //    logic): `x NOT IN (1, NULL)` is NULL, not TRUE, for every x != 1.
  //    Spark optimises NOT IN into an anti-join, so it is worth *looking*
  //    at the plan instead of trusting the textbook.
  // =====================================================================
  println(label + "3. IN / NOT IN and the NULL trap")

  spark.sql(
    """
    |SELECT cust_id, cust_name FROM customers WHERE cust_id IN (SELECT cust_id FROM orders)
    |ORDER BY cust_id
  """.stripMargin).show(false)

  val noOrderCustIds = Seq(1, 2, null.asInstanceOf[Int])
  spark.createDataset(noOrderCustIds).toDF("cust_id").createOrReplaceTempView("cust_id_list")

  // the textbook failure: NOT IN a list containing NULL returns nothing
  spark.sql(
    """
    |SELECT cust_id, cust_name
    |FROM customers
    |WHERE cust_id NOT IN (SELECT cust_id FROM cust_id_list)
    |ORDER BY cust_id
    |LIMIT 5
  """.stripMargin).show(false)
  val nullListCount = spark.sql(
    "SELECT COUNT(*) FROM customers WHERE cust_id NOT IN (SELECT cust_id FROM cust_id_list)").head().getLong(0)
  println("NOT IN with a NULL in the list -> " + nullListCount +
    " rows. The textbook answer is 0 (three-valued logic); Spark rewrites NOT IN " +
    "into a LeftAnti join, which skips the NULL and returns the customers that " +
    "are not in {1, 2} — different engines, different answers.")

  // the fix: make the list NULL-free first
  spark.sql(
    """
    |SELECT cust_id, cust_name
    |FROM customers
    |WHERE cust_id NOT IN (
    |  SELECT cust_id FROM cust_id_list WHERE cust_id IS NOT NULL
    |)
    |ORDER BY cust_id
  """.stripMargin).show(false)
  println("NOT IN with WHERE cust_id IS NOT NULL -> " +
    spark.sql("SELECT COUNT(*) FROM customers WHERE cust_id NOT IN " +
      "(SELECT cust_id FROM cust_id_list WHERE cust_id IS NOT NULL)").head().getLong(0) + " rows")

  // what the planner actually built for NOT IN
  val notInDf = spark.sql(
    """
    |SELECT cust_id, cust_name
    |FROM customers
    |WHERE cust_id NOT IN (
    |  SELECT cust_id FROM cust_id_list WHERE cust_id IS NOT NULL
    |)
  """.stripMargin)
  println("plan for NOT IN: " + notInDf.queryExecution.executedPlan.getClass.getSimpleName)
  notInDf.queryExecution.executedPlan.toString.linesIterator
    .filter(l => l.contains("Join") || l.contains("Filter") || l.contains("Build"))
    .foreach(l => println("  " + l.trim.take(120)))

  // rdd — the "IN" list as a broadcast Set. A null member poisons the Set
  //     test, so drop nulls explicitly.
  val allowedCustIds: Set[Long] = spark.table("cust_id_list")
    .filter(col("cust_id").isNotNull)
    .as[Long].collect().toSet
  customers.rdd
    .filter(row => !allowedCustIds.contains(row.getAs[Long]("cust_id")))
    .map(row => (row.getAs[Long]("cust_id"), row.getAs[String]("cust_name")))
    .collect()
    .sortBy(_._1)
    .foreach(println)

  // dataframe — left_anti join is the honest spelling of NOT EXISTS
  customers.join(ordersL.select("cust_id").distinct(), Seq("cust_id"), "left_anti")
    .select("cust_id", "cust_name")
    .orderBy("cust_id")
    .show(false)

  // dataset
  customers.as[datasets.Customer]
    .map(c => (c.cust_id, c.cust_name))
    .toDF("cust_id", "cust_name")
    .join(ordersL.select("cust_id").distinct(), Seq("cust_id"), "left_anti")
    .orderBy("cust_id")
    .show(false)

  // =====================================================================
  // 4. EXISTS / NOT EXISTS — the correlated, row-at-a-time form
  // =====================================================================
  println(label + "4. EXISTS / NOT EXISTS (correlated)")

  spark.sql(
    """
    |SELECT c.cust_id, c.cust_name
    |FROM customers c
    |WHERE EXISTS (SELECT 1 FROM orders o WHERE o.cust_id = c.cust_id)
    |ORDER BY c.cust_id
  """.stripMargin).show(false)

  spark.sql(
    """
    |SELECT c.cust_id, c.cust_name
    |FROM customers c
    |WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.cust_id = c.cust_id)
    |ORDER BY c.cust_id
  """.stripMargin).show(false)

  // the plan for EXISTS — a semi join
  val existsDf = spark.sql(
    """
    |SELECT c.cust_id FROM customers c
    |WHERE EXISTS (SELECT 1 FROM orders o WHERE o.cust_id = c.cust_id)
  """.stripMargin)
  existsDf.queryExecution.executedPlan.toString.linesIterator
    .filter(_.contains("Join"))
    .foreach(l => println("  " + l.trim.take(120)))

  // rdd — collect the small side, then test membership per row
  val orderingCustIds: Set[Long] = ordersL.map(_.getAs[Long]("cust_id")).distinct().collect().toSet
  customers.rdd
    .filter(row => orderingCustIds.contains(row.getAs[Long]("cust_id")))
    .map(row => (row.getAs[Long]("cust_id"), row.getAs[String]("cust_name")))
    .collect()
    .sortBy(_._1)
    .foreach(println)

  // dataframe
  customers.join(ordersL.select("cust_id").distinct(), Seq("cust_id"), "left_semi")
    .orderBy("cust_id")
    .show(false)

  // dataset
  customers.as[datasets.Customer]
    .filter(c => orderingCustIds.contains(c.cust_id))
    .show(false)

  // =====================================================================
  // 5. Correlated scalar subquery — one value per outer row
  // =====================================================================
  println(label + "5. Correlated scalar subquery")

  // A correlated scalar subquery in the SELECT list fails in Spark 3.5.1 when
  // the outer relation is a UNION (the decorrelated plan trips
  // PushProjectionThroughUnion). `customers` is a union of two frames, so the
  // example runs on a flattened copy — and the failure is shown, not hidden.
  val customersFlat = spark.createDataFrame(customerAll.rdd, customerAll.schema)
  customersFlat.createOrReplaceTempView("customers_flat")

  spark.sql(
    """
    |SELECT c.cust_id, c.cust_name,
    |       (SELECT COUNT(*) FROM orders_c o WHERE o.cust_id = c.cust_id)         AS order_cnt,
    |       (SELECT MAX(o.order_date) FROM orders_c o WHERE o.cust_id = c.cust_id) AS last_order_date
    |FROM customers_flat c
    |ORDER BY order_cnt DESC, c.cust_id
    |LIMIT 10
  """.stripMargin).show(false)

  try {
    spark.sql(
      """
      |SELECT c.cust_id,
      |       (SELECT COUNT(*) FROM orders_c o WHERE o.cust_id = c.cust_id) AS order_cnt
      |FROM customers c
    """.stripMargin).show(1)
  } catch {
    case e: Throwable =>
      println("same subquery over the union-backed view -> " + e.getClass.getSimpleName + ": " + e.getMessage)
  }

  // the WHERE-clause form decorrelates fine even on the union view
  spark.sql(
    """
    |SELECT c.cust_id
    |FROM customers c
    |WHERE (SELECT COUNT(*) FROM orders_c o WHERE o.cust_id = c.cust_id) > 1
    |ORDER BY c.cust_id
  """.stripMargin).show(false)

  // the decorrelated plan: the subquery became a join
  val correlatedPlan = spark.sql(
    """
    |SELECT c.cust_id
    |FROM customers c
    |WHERE (SELECT COUNT(*) FROM orders_c o WHERE o.cust_id = c.cust_id) > 1
  """.stripMargin)
  println("correlated subquery became: " +
    correlatedPlan.queryExecution.optimizedPlan.toString.linesIterator
      .filter(line => line.contains("Join") || line.contains("Subquery"))
      .map(_.trim.take(100)).mkString(" | "))

  // rdd — group the small side, then map
  val orderCntByCust: Map[Long, Long] =
    ordersL.rdd.map(row => (row.getAs[Long]("cust_id"), 1L)).reduceByKey(_ + _).collect().toMap
  customers.rdd
    .map(row => (row.getAs[Long]("cust_id"), orderCntByCust.getOrElse(row.getAs[Long]("cust_id"), 0L)))
    .collect()
    .sortBy(x => (-x._2, x._1))
    .take(10)
    .foreach(println)

  // dataframe — a left join + groupBy is the explicit form of the correlated
  // subquery, and the plan for the subquery is exactly this
  customers.join(ordersL.groupBy("cust_id").agg(count(lit(1)).as("order_cnt"),
      max("order_date").as("last_order_date")),
    Seq("cust_id"), "left")
    .orderBy(desc("order_cnt"), asc("cust_id"))
    .show(10, false)

  // dataset
  customers.as[datasets.Customer]
    .map(c => (c.cust_id, c.cust_name, orderCntByCust.getOrElse(c.cust_id, 0L)))
    .toDF("cust_id", "cust_name", "order_cnt")
    .orderBy(desc("order_cnt"), asc("cust_id"))
    .show(10, false)

  // =====================================================================
  // 6. Derived table in FROM — a subquery you can GROUP BY
  // =====================================================================
  println(label + "6. Derived table in FROM")

  spark.sql(
    """
    |SELECT city, SUM(cust_cnt) AS cust_cnt
    |FROM (
    |  SELECT c.city, c.cust_id, COUNT(o.order_id) AS cust_cnt
    |  FROM customers c
    |  LEFT JOIN orders o ON c.cust_id = o.cust_id
    |  GROUP BY c.city, c.cust_id
    |) per_customer
    |GROUP BY city
    |ORDER BY city
  """.stripMargin).show(false)

  // rdd — the inner "group by" is the same work with tuples
  val custCity = customers.rdd.map(row => (row.getAs[Long]("cust_id"), row.getAs[String]("city")))
    .collect().toMap
  val cntByCust = ordersL.rdd.map(row => (row.getAs[Long]("cust_id"), 1L)).reduceByKey(_ + _).collect().toMap
  custCity.toSeq
    .map { case (custId, city) => (city, cntByCust.getOrElse(custId, 0L)) }
    .groupBy(_._1)
    .map { case (city, rows) => (city, rows.map(_._2).sum) }
    .toSeq.sortBy(_._1).foreach(println)

  // dataframe
  val perCustomer = customers
    .join(ordersL, Seq("cust_id"), "left")
    .groupBy("city", "cust_id")
    .agg(count("order_id").as("cust_cnt"))
  perCustomer.groupBy("city").agg(sum("cust_cnt").as("cust_cnt")).show(false)

  // dataset
  perCustomer.as[(String, Long, Long)]
    .map { case (city, custId, cnt) => (city, custId, cnt) }
    .toDF("city", "cust_id", "cust_cnt")
    .groupBy("city").agg(sum("cust_cnt").as("cust_cnt"))
    .show(false)

  // =====================================================================
  // 7. No LATERAL / no TOP 1 PER GROUP via subquery — emulate it
  //    row_number() in a window, or a groupBy + a join back to the key.
  // =====================================================================
  println(label + "7. Latest order per customer (no LATERAL in Spark)")

  // there is no LATERAL / "TOP 1 PER GROUP" in Spark SQL; the window version
  // is the idiomatic replacement and stays a single shuffle
  val latestOrders = orders
    .withColumn("rn", row_number().over(Window.partitionBy("cust_id").orderBy(desc("order_date"), desc("order_id"))))
    .filter(col("rn") === 1)
    .drop("rn")
  latestOrders.orderBy("cust_id").show(false)

  // rdd — groupByKey().reduce(max by date)
  orders.rdd
    .map(row => (row.getAs[Int]("cust_id"), row))
    .groupByKey()
    .map { case (_, rows) =>
      rows.reduce((a, b) =>
        if (a.getAs[java.sql.Date]("order_date").after(b.getAs[java.sql.Date]("order_date"))) a else b)
    }
    .map(row => (row.getAs[Int]("cust_id"), row.getAs[Int]("order_id"), row.getAs[java.sql.Date]("order_date")))
    .collect()
    .sortBy(_._1)
    .foreach(println)

  // dataset
  orders.as[datasets.Order]
    .groupByKey(_.cust_id)
    .reduceGroups((a: datasets.Order, b: datasets.Order) =>
      if (a.order_date.after(b.order_date)) a else b)
    .map(_._2)
    .map(o => (o.cust_id, o.order_id, o.order_date))
    .toDF("cust_id", "order_id", "order_date")
    .orderBy("cust_id")
    .show(false)

  // =====================================================================
  // 8. The "collect the keys, then join" trap, and what it costs
  //     A subquery is not automatically cheaper: a hand-rolled broadcast join
  //     of the same predicate shows up in the plan as an actual join, and the
  //     subquery is decorrelated into that very join.
  // =====================================================================
  println(label + "8. Subquery vs join in the plan")

  orders.persist(StorageLevel.MEMORY_AND_DISK)
  orders.count()

  val viaSubquery = spark.sql(
    "SELECT cust_id, COUNT(*) FROM orders WHERE cust_id IN (SELECT cust_id FROM orders) GROUP BY cust_id")
  val viaJoin = orders.as("o").join(orders.select("cust_id").distinct().as("k"), $"o.cust_id" === $"k.cust_id")
    .groupBy("o.cust_id").agg(count(lit(1)))

  def joinLines(df: DataFrame): Seq[String] = {
    val plan = df.queryExecution.executedPlan match {
      case a: AdaptiveSparkPlanExec => a.executedPlan
      case p => p
    }
    plan.toString.linesIterator.filter(_.contains("Join")).map(_.trim.take(90)).toSeq
  }
  println("IN-subquery plan:  " + joinLines(viaSubquery).mkString(" | "))
  println("hand-join plan:    " + joinLines(viaJoin).mkString(" | "))

  // the anti-pattern: pulling every distinct key to the driver and filtering
  val distinctCustIds: Array[Int] = orders.select("cust_id").distinct().collect().map(_.getInt(0))
  println("distinct cust_ids collected to the driver: " + distinctCustIds.sorted.mkString(","))

  orders.unpersist()
  spark.stop()
}
