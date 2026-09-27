package rddTransformations

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import datasets._

/**
 * 07 - RDD-only transformations that DataFrames/Datasets abstract away:
 * narrow vs wide ops, groupByKey vs reduceByKey vs aggregateByKey,
 * mapPartitions, coalesce/repartition, partitioning, accumulators and
 * broadcast variables.
 *
 * SQL / DataFrame / Dataset equivalents are shown only where a direct
 * counterpart exists; many of these operations are RDD-specific.
 */
object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("07-rdd-transformations")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)
  customerAll.createOrReplaceTempView("customers")
  orders.createOrReplaceTempView("orders")

  val sc: SparkContext = spark.sparkContext

  /**
   * map / flatMap / filter — the most common narrow transformations.
   */
  val nums: RDD[Int] = sc.parallelize(1 to 20, 4)
  nums.map(_ * 2).collect().foreach(println)
  nums.flatMap(x => Seq(x, x * 10)).collect().foreach(println)
  nums.filter(_ % 2 == 0).collect().foreach(println)

  // sql (temp view)
  spark.range(1, 21).createOrReplaceTempView("nums")
  spark.sql("SELECT id * 2 AS doubled FROM nums WHERE id % 2 = 0").show(false)

  // dataframe
  spark.range(1, 21)
    .withColumn("doubled", col("id") * 2)
    .filter(col("id") % 2 === 0)
    .show(false)

  // dataset
  spark.range(1, 21).as[Long]
    .map(_ * 2)
    .filter(_ % 2 == 0)
    .show(false)


  /**
   * groupByKey vs reduceByKey vs aggregateByKey.
   * groupByKey shuffles ALL values; reduceByKey/aggregateByKey combine
   * values locally (map-side combine) before the shuffle.
   */
  val kv: RDD[(String, Int)] = nums.map(x => (if (x % 2 == 0) "even" else "odd", 1))

  // groupByKey — computes at the driver, fine for small groups only
  val grouped = kv.groupByKey().mapValues(_.sum)
  grouped.collect().foreach(println)

  // reduceByKey — map-side combine then shuffle
  kv.reduceByKey(_ + _).collect().foreach(println)

  // aggregateByKey — combine with a zero value of a different type
  kv.aggregateByKey(0)((acc, v) => acc + v, _ + _).collect().foreach(println)

  // dataframe
  nums.toDF("x").withColumn("parity", when(col("x") % 2 === 0, "even").otherwise("odd"))
    .groupBy("parity").count().show(false)

  // dataset
  nums.toDS()
    .map(x => (if (x % 2 == 0) "even" else "odd", 1))
    .groupByKey(_._1)
    .reduceGroups((a, b) => (a._1, a._2 + b._2))
    .show(false)


  /**
   * mapPartitions — run expensive setup (e.g. opening a connection)
   * once per partition instead of once per row.
   */
  // sql / dataframe / dataset — no direct row-level equivalent; closest is a
  // groupBy/mapGroups pattern, or just accept the per-row cost
  nums
    .mapPartitions(iter => {
      val perPartitionSetup = iter.sum
      Iterator(perPartitionSetup)
    })
    .collect()
    .foreach(println)

  // dataset — mapGroups is the typed analogue when you group first
  nums.toDS()
    .groupByKey(_  => 0L) // group everything into one key to emulate a partition
    .mapGroups { case (_, numsIter) => numsIter.sum }
    .show(false)


  /**
   * coalesce vs repartition. coalesce reuses existing partitions and can
   * only shrink (unless shuffle=true); repartition always shuffles.
   */
  println("partitions before : " + nums.getNumPartitions)
  val shrunk = nums.coalesce(2)
  println("coalesce(2)      : " + shrunk.getNumPartitions)
  val expanded = nums.repartition(8)
  println("repartition(8)   : " + expanded.getNumPartitions)

  // dataframe
  nums.toDF("x").coalesce(2).rdd.getNumPartitions
  customerAll.repartition(8, "city").rdd.getNumPartitions
  println("df repartition by city -> " + customerAll.repartition(8, "city").rdd.getNumPartitions)


  /**
   * Custom partitioning — group rows with the same key into the same
   * partition to avoid shuffles in later joins. RangePartitioner gives
   * roughly balanced partitions.
   */
  val partitioned = kv.partitionBy(new org.apache.spark.RangePartitioner(4, kv))
  println("partitioner       : " + partitioned.partitioner)
  partitioned.glom().map(_.toSeq).collect().foreach(part => println("partition has " + part.size + " elements"))


  /**
   * Accumulators — driver-side counters updated from executors.
   */
  val errors = new LongAccumulator
  sc.register(errors, "errorsAcc")
  val lines = sc.parallelize(Seq("ok:200", "ok:200", "err:500", "ok:200"))
  lines.foreach { l =>
    if (l.startsWith("err")) errors.add(1L)
  }
  println("accumulator value  : " + errors.value)

  // custom AccumulatorV2 (e.g. collecting max values in a Map)
  class MapMaxAcc extends AccumulatorV2[(String, Int), Map[String, Int]] {
    private var m = Map.empty[String, Int]
    override def isZero: Boolean = m.isEmpty
    override def copy(): AccumulatorV2[(String, Int), Map[String, Int]] = { val a = new MapMaxAcc; a.m = m; a }
    override def reset(): Unit = m = Map.empty
    override def add(v: (String, Int)): Unit = m = m.updated(v._1, math.max(v._2, m.getOrElse(v._1, Int.MinValue)))
    override def merge(other: AccumulatorV2[(String, Int), Map[String, Int]]): Unit = other.value.foreach { case (k, v) => m = m.updated(k, math.max(v, m.getOrElse(k, Int.MinValue))) }
    override def value: Map[String, Int] = m
  }
  val maxes = new MapMaxAcc
  sc.register(maxes)
  nums.map(x => ("group" + x % 3, x)).foreach(maxes.add)
  println("custom accumulator: " + maxes.value)


  /**
   * Broadcast variables — ship a large read-only lookup to each executor
   * once, instead of serializing it with every task.
   */
  val cityLookup: Map[Long, String] = customerAll
    .select("cust_id", "city")
    .collect()
    .map(r => r.getLong(0) -> r.getString(1))
    .toMap
  val cityBc = sc.broadcast(cityLookup)

  val enriched = sc.parallelize(Seq(1L, 2L, 3L)).map(id => (id, cityBc.value.getOrElse(id, "unknown")))
  enriched.collect().foreach(println)

  // sql / dataframe / dataset — the equivalent is just a broadcast join hint
  spark.sql("SELECT /*+ BROADCAST(c) */ c.cust_id, c.city, o.amount FROM customers c LEFT JOIN orders o ON c.cust_id = o.cust_id").show(false)

  // dataframe
  customerAll.join(orders.hint("broadcast"), "cust_id").show(false)

  /**
   * sortByKey on RDD partitions data by key for efficiency.
   */
  kv.sortByKey().collect().foreach(println)


  /**
   * Set operations on RDDs.
   */
  val a = sc.parallelize(Seq(1, 2, 3, 4))
  val b = sc.parallelize(Seq(3, 4, 5, 6))
  a.union(b).distinct().collect().foreach(println)
  a.intersection(b).collect().foreach(println)
  a.subtract(b).collect().foreach(println)
  a.cartesian(b).collect().foreach(println)

  spark.stop()
}