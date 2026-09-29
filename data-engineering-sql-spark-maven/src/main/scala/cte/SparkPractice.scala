package cte

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import datasets._

// Local hierarchy data for the "recursive CTE" section (an org tree).
// A root row has mgr_id = -1 so the column stays non-null and joinable.
case class Employee(emp_id: Long, mgr_id: Long, emp_name: String, dept: String)
case class PathNode(node_id: Long, path: String, lvl: Int)

/**
 * 14 - CTEs: one named step, chained steps, a CTE referenced more than once,
 * CTEs combined with windows, and hierarchy traversal.
 *
 * Three things to take away:
 *   1. A CTE is a *naming* device, not a materialisation. Spark substitutes
 *      (inlines) the body at every reference site, so a CTE used twice is
 *      computed twice — `explain` shows no "CTE" node, just the body twice.
 *   2. There is no CTE in the DataFrame/Dataset API. The equivalent is a temp
 *      view (a named plan, also inlined) or a cached DataFrame (materialised).
 *   3. Spark's parser has no `WITH RECURSIVE` and a self-referencing CTE is
 *      rejected during analysis, so traversal is done with an iterative loop.
 */
object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("14-common-table-expressions")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)
  customerAll.createOrReplaceTempView("customers")
  orders.createOrReplaceTempView("orders")

  val employees = Seq(
    (1L, -1L, "asha", "corp"),
    (2L, 1L, "bharat", "eng"),
    (3L, 1L, "chandra", "fin"),
    (4L, 2L, "deepa", "eng"),
    (5L, 4L, "esha", "eng"),
    (6L, 3L, "farhan", "fin")
  ).toDF("emp_id", "mgr_id", "emp_name", "dept")
  employees.createOrReplaceTempView("employees")


  /**
   * 1. One CTE, one logical step. Nothing is faster here — the value is that
   *    the query reads like a pipeline and the sub-select no longer needs a
   *    made-up alias.
   */
  // sql (temp view)
  spark.sql("""
    WITH completed AS (
      SELECT * FROM orders WHERE status = 'Completed'
    )
    SELECT cust_id, SUM(amount) AS completed_amt, COUNT(*) AS orders_cnt
    FROM completed
    GROUP BY cust_id
    ORDER BY completed_amt DESC
  """).show(false)

  // rdd — keep the status, filter, then aggregate by key
  orders.rdd
    .map(row => (row.getAs[Int]("cust_id"), (row.getAs[Double]("amount"), 1, row.getAs[String]("status"))))
    .filter { case (_, (amt, _, status)) => amt > 0 && status == "Completed" }
    .map { case (k, (amt, cnt, _)) => (k, (amt, cnt)) }
    .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    .collect()
    .sortBy(-_._2._1)
    .foreach(println)

  // dataframe — the CTE becomes a val
  val completed = orders.filter(col("status") === "Completed")
  completed
    .groupBy("cust_id")
    .agg(sum("amount").alias("completed_amt"), count(lit(1)).alias("orders_cnt"))
    .orderBy(desc("completed_amt"))
    .show(false)

  // dataset — typed filter, then a typed reduce. (agg with named columns is a
  // DataFrame-only shape: agg() on a KeyValueGroupedDataset wants TypedColumns
  // and .alias() hands back a plain Column, so reduce the typed rows and name
  // the output with toDF. reduceGroups gives (key, row) pairs — map(_._2) first.)
  orders.as[Order]
    .filter(_.status == "Completed")
    .groupByKey(_.cust_id)
    .reduceGroups((a: Order, b: Order) => a.copy(amount = a.amount + b.amount))
    .map(_._2)
    .toDF("order_id", "cust_id", "order_date", "completed_amt", "status")
    .orderBy(desc("completed_amt"))
    .show(false)


  /**
   * 2. Chained CTEs: the four-step pipeline (clean -> dedupe -> aggregate ->
   *    rank) written once, top to bottom. Later steps can only read earlier
   *    ones — that is the whole contract.
   */
  // sql (temp view)
  spark.sql("""
    WITH valid_orders AS (
      SELECT order_id, cust_id, order_date, amount, status
      FROM orders
      WHERE amount > 0 AND status IS NOT NULL
    ),
    one_row_per_order AS (
      SELECT order_id, cust_id, order_date, amount,
             ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) AS rn
      FROM valid_orders
    ),
    customer_totals AS (
      SELECT cust_id, COUNT(*) AS order_cnt, SUM(amount) AS total_amt
      FROM one_row_per_order
      WHERE rn = 1
      GROUP BY cust_id
    )
    SELECT cust_id, order_cnt, total_amt,
           DENSE_RANK() OVER (ORDER BY total_amt DESC) AS amt_rank
    FROM customer_totals
    ORDER BY amt_rank, cust_id
  """).show(false)

  // rdd — same four steps without windows: dedupe per key, aggregate, sort
  val valid = orders.rdd.filter(row => row.getAs[Double]("amount") > 0 && row.getAs[String]("status") != null)
  val deduped = valid
    .map(row => (row.getAs[Int]("order_id"), row))
    .reduceByKey((a, b) =>
      if (a.getAs[java.sql.Date]("order_date").after(b.getAs[java.sql.Date]("order_date"))) a else b)
    .map(_._2)
  val totals = deduped
    .map(row => (row.getAs[Int]("cust_id"), (1, row.getAs[Double]("amount"))))
    .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
    .collect()
    .sortBy(-_._2._2)
  totals.zipWithIndex.foreach { case ((custId, (cnt, amt)), i) => println(s"$custId $cnt $amt rank=${i + 1}") }

  // dataframe — each CTE is a val, chained by name
  val validOrders = orders.filter(col("amount") > 0 && col("status").isNotNull)
  val oneRowPerOrder = validOrders
    .withColumn("rn", row_number().over(Window.partitionBy("order_id").orderBy(desc("order_date"))))
  val customerTotals = oneRowPerOrder.filter(col("rn") === 1)
    .groupBy("cust_id")
    .agg(count(lit(1)).alias("order_cnt"), sum("amount").alias("total_amt"))
  customerTotals
    .withColumn("amt_rank", dense_rank().over(Window.orderBy(desc("total_amt"))))
    .orderBy(asc("amt_rank"), asc("cust_id"))
    .show(false)

  // dataset
  orders.as[Order]
    .filter(o => o.amount > 0 && o.status != null)
    .groupByKey(_.cust_id)
    .reduceGroups((a: Order, b: Order) => a.copy(amount = a.amount + b.amount))
    .map(_._2)
    .toDF("order_id", "cust_id", "order_date", "total_amt", "status")
    .withColumn("amt_rank", dense_rank().over(Window.orderBy(desc("total_amt"))))
    .orderBy(asc("amt_rank"), asc("cust_id"))
    .show(false)


  /**
   * 3. A CTE used twice is inlined twice, so the work happens twice. The
   *    DataFrame-level fix is `cache()`: the plan then collapses to an
   *    InMemoryRelation. A temp view alone does not help — it is still a plan.
   */
  val reusedSql = """
    WITH heavy AS (
      SELECT cust_id, COUNT(*) AS order_cnt, SUM(amount) AS total_amt
      FROM orders
      GROUP BY cust_id
    )
    SELECT a.cust_id, a.total_amt, b.order_cnt
    FROM heavy a JOIN heavy b ON a.cust_id = b.cust_id
    WHERE a.total_amt > b.order_cnt
  """
  // sql (temp view) — the aggregation shows up in both join branches
  spark.sql(reusedSql).explain("extended")
  spark.sql(reusedSql).show(false)

  // dataframe — cache() changes the physical plan
  val heavy = orders.groupBy("cust_id").agg(count(lit(1)).alias("order_cnt"), sum("amount").alias("total_amt"))
  heavy.cache()
  heavy.count() // populate the cache before the second branch scans it
  heavy.as("a").join(heavy.as("b"), col("a.cust_id") === col("b.cust_id"))
    .filter(col("a.total_amt") > col("b.order_cnt"))
    .explain("extended")
  heavy.unpersist()

  // dataset — same thing, cached before the self-join
  val heavyDs = orders.as[Order]
    .groupByKey(_.cust_id)
    .reduceGroups((a: Order, b: Order) => a.copy(amount = a.amount + b.amount))
    .map(_._2)
    .toDF("order_id", "cust_id", "order_date", "total_amt", "status")
    .withColumn("order_cnt", count(lit(1)).over(Window.partitionBy("cust_id")))
    .cache()
  heavyDs.count()
  heavyDs.as("a").join(heavyDs.as("b"), col("a.cust_id") === col("b.cust_id"))
    .filter(col("a.total_amt") > col("b.order_cnt"))
    .explain("extended")
  heavyDs.unpersist()


  /**
   * 4. CTE + window: per-customer totals first, then a running total and a
   *    rank over those totals. The CTE stops the aggregate from being written
   *    twice in the same query.
   */
  // sql (temp view)
  spark.sql("""
    WITH customer_amt AS (
      SELECT cust_id, SUM(amount) AS total_amt, MAX(order_date) AS last_order_date
      FROM orders
      GROUP BY cust_id
    )
    SELECT cust_id, total_amt, last_order_date,
           SUM(total_amt) OVER (ORDER BY total_amt DESC
                                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amt,
           RANK() OVER (ORDER BY total_amt DESC) AS amt_rank
    FROM customer_amt
    ORDER BY amt_rank
  """).show(false)

  // rdd — aggregate, then scan a prefix (what the window does, by hand)
  val totalsRdd = orders.rdd
    .map(row => (row.getAs[Int]("cust_id"), (row.getAs[Double]("amount"), row.getAs[java.sql.Date]("order_date").getTime)))
    .reduceByKey((a, b) => (a._1 + b._1, math.max(a._2, b._2)))
    .collect()
    .sortBy(-_._2._1)
  var running = 0.0
  totalsRdd.zipWithIndex.foreach { case ((custId, (amt, lastMs)), i) =>
    running += amt
    println(s"$custId total=$amt rank=${i + 1} running=$running last=${new java.sql.Date(lastMs)}")
  }

  // dataframe
  val customerAmt = orders.groupBy("cust_id")
    .agg(sum("amount").alias("total_amt"), max("order_date").alias("last_order_date"))
  customerAmt
    .withColumn("running_amt", sum("total_amt").over(
      Window.orderBy(desc("total_amt")).rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    .withColumn("amt_rank", rank().over(Window.orderBy(desc("total_amt"))))
    .orderBy(asc("amt_rank"))
    .show(false)

  // dataset
  orders.as[Order]
    .groupByKey(_.cust_id)
    .reduceGroups((a: Order, b: Order) => a.copy(amount = a.amount + b.amount))
    .map(_._2)
    .toDF("order_id", "cust_id", "order_date", "total_amt", "status")
    .withColumn("amt_rank", rank().over(Window.orderBy(desc("total_amt"))))
    .orderBy(asc("amt_rank"))
    .show(false)


  /**
   * 5. Recursive CTE. Spark has no `WITH RECURSIVE`, and a self-referencing
   *    CTE is rejected during analysis. The three real options:
   *      a. a fixed-depth self-join (one join per level, fine for shallow trees)
   *      b. an iterative loop until the frontier is empty (any depth, one job
   *         per level)
   *      c. an RDD breadth-first search to a fixed point (same idea, no Catalyst)
   *    The failing SQL is shown first so the error is not a surprise.
   */
  // sql — the syntax Spark wants does not exist
  try {
    spark.sql("""
      WITH RECURSIVE emp_tree AS (
        SELECT emp_id, mgr_id, emp_name, 0 AS lvl FROM employees WHERE mgr_id < 0
        UNION ALL
        SELECT e.emp_id, e.mgr_id, e.emp_name, t.lvl + 1
        FROM employees e JOIN emp_tree t ON e.mgr_id = t.emp_id
      )
      SELECT * FROM emp_tree
    """).show(false)
  } catch {
    case e: Throwable => println("WITH RECURSIVE -> " + e.getMessage.take(160))
  }

  // sql — (a) fixed depth, one self-join per level
  spark.sql("""
    WITH l0 AS (SELECT emp_id, mgr_id, emp_name, 0 AS lvl FROM employees WHERE mgr_id < 0),
         l1 AS (SELECT e.emp_id, e.mgr_id, e.emp_name, 1 AS lvl FROM employees e JOIN l0 ON e.mgr_id = l0.emp_id),
         l2 AS (SELECT e.emp_id, e.mgr_id, e.emp_name, 2 AS lvl FROM employees e JOIN l1 ON e.mgr_id = l1.emp_id),
         l3 AS (SELECT e.emp_id, e.mgr_id, e.emp_name, 3 AS lvl FROM employees e JOIN l2 ON e.mgr_id = l2.emp_id)
    SELECT * FROM l0
    UNION ALL SELECT * FROM l1
    UNION ALL SELECT * FROM l2
    UNION ALL SELECT * FROM l3
    ORDER BY lvl, emp_id
  """).show(false)

  // (b) dataframe — iterate until the frontier is empty
  var frontier: DataFrame = employees.filter(col("mgr_id") < 0)
    .select(col("emp_id").as("node_id"), col("emp_name").cast("string").as("path"), lit(0).as("lvl"))
  var walked: DataFrame = frontier
  var depth = 0
  while (frontier.count() > 0 && depth < 10) {
    depth += 1
    val next = employees.as("e")
      .join(frontier.as("c"), col("e.mgr_id") === col("c.node_id"))
      .select(
        col("e.emp_id").as("node_id"),
        concat(col("c.path"), lit(" > "), col("e.emp_name")).as("path"),
        lit(depth).as("lvl"))
    walked = walked.union(next)
    frontier = next
  }
  walked.orderBy(asc("lvl"), asc("node_id")).show(false)

  // (c) rdd — breadth-first search to a fixed point. The edge index is
  // collected to the driver (a graph is small on the "index" side); each
  // round is one lookup pass, no job per level beyond the collect.
  val childrenByManager = employees.rdd
    .filter(row => row.getAs[Long]("mgr_id") >= 0)
    .map(row => (row.getAs[Long]("mgr_id"), (row.getAs[Long]("emp_id"), row.getAs[String]("emp_name"))))
    .groupByKey()
    .collectAsMap()
  var rddFrontier = employees.rdd
    .filter(row => row.getAs[Long]("mgr_id") < 0)
    .map(row => (row.getAs[Long]("emp_id"), row.getAs[String]("emp_name")))
    .collect()
    .toSet
  var visitedIds = rddFrontier.map(_._1).toSet
  var rddLevel = 0
  while (rddFrontier.nonEmpty && rddLevel < 10) {
    rddLevel += 1
    val nextLevel = rddFrontier.toSeq.flatMap { case (nodeId, path) =>
      childrenByManager.getOrElse(nodeId, Iterable.empty).collect {
        case (childId, childName) if !visitedIds.contains(childId) => (childId, path + " > " + childName)
      }
    }.toSet
    nextLevel.toSeq.sortBy(_._1).foreach { case (nodeId, path) => println(s"lvl=$rddLevel $nodeId $path") }
    visitedIds ++= nextLevel.map(_._1)
    rddFrontier = nextLevel
  }

  // dataset — the same loop with typed rows
  var dsFrontier: Dataset[PathNode] = employees
    .filter(col("mgr_id") < 0)
    .select(col("emp_id").as("node_id"), col("emp_name").cast("string").as("path"), lit(0).as("lvl"))
    .as[PathNode]
  var dsWalked = dsFrontier
  var dsDepth = 0
  while (dsFrontier.count() > 0 && dsDepth < 10) {
    dsDepth += 1
    val next = employees
      .join(dsFrontier, col("mgr_id") === col("node_id"))
      .select(
        col("emp_id").as("node_id"),
        concat(col("path"), lit(" > "), col("emp_name")).as("path"),
        lit(dsDepth).as("lvl"))
      .as[PathNode]
    dsWalked = dsWalked.union(next)
    dsFrontier = next
  }
  dsWalked.orderBy("lvl", "node_id").show(false)


  /**
   * 6. WITH also prefixes INSERT OVERWRITE / CREATE TABLE AS, so staging and
   *    the write stay in one statement. The CTE is still inlined into the
   *    write, which is why "materialise, then write" is often the safer shape
   *    when the same subquery feeds two different outputs.
   */
  // sql — stage into a view, then read it twice (the CTE would be inlined twice)
  spark.sql("""
    WITH per_city AS (
      SELECT city, COUNT(*) AS cust_cnt
      FROM customers
      WHERE city IS NOT NULL
      GROUP BY city
    )
    SELECT city, cust_cnt FROM per_city WHERE cust_cnt > 100
  """).createOrReplaceTempView("big_cities")
  spark.sql("SELECT * FROM big_cities ORDER BY cust_cnt DESC").show(20, false)
  spark.sql("SELECT COUNT(*) AS n_big_cities FROM big_cities").show(false)

  // rdd — the grouped-and-filtered RDD is the staged result
  customerAll.rdd
    .map(row => (row.getAs[String]("city"), 1))
    .reduceByKey(_ + _)
    .filter { case (_, c) => c > 100 }
    .collect()
    .sortBy(-_._2)
    .take(20)
    .foreach(println)

  // dataframe
  customerAll.groupBy("city").agg(count(lit(1)).alias("cust_cnt"))
    .filter(col("cust_cnt") > 100)
    .orderBy(desc("cust_cnt"))
    .show(20, false)

  // dataset
  customerAll.as[Customer]
    .groupByKey(_.city)
    .count()
    .filter(pair => pair._2 > 100)
    .show(20, false)

  spark.stop()
}
