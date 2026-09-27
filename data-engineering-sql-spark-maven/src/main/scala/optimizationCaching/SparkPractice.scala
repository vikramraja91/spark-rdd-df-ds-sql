package optimizationCaching

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import datasets._

case class StateRow(city: String, state: String)

object SparkPractice extends App {

  // AQE (adaptive query execution) is ON by default in Spark 3.5.
  val spark = SparkSession.builder()
    .appName("12-optimization-and-caching")
    .master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", "10m") // broadcast joins below this size
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)
  customerAll.createOrReplaceTempView("customers")
  orders.createOrReplaceTempView("orders")

  /**
   * Explain plans: physical plan shows the shuffles / joins actually run.
   */
  spark.sql("SELECT c.city, COUNT(*) FROM customers c JOIN orders o ON c.cust_id = o.cust_id GROUP BY c.city").explain("extended")

  // dataframe
  customerAll.join(orders, "cust_id").groupBy("city").count().explain("extended")


  /**
   * Caching — DFS (memory/disk) materialization so a reused dataset is not
   * recomputed on every action. Cache is lazy: it only populates when an
   * action (count/show) runs on it.
   */
  val cachedCustomers = customerAll.rdd.persist(StorageLevel.MEMORY_AND_DISK)
  cachedCustomers.count() // triggers the cache
  cachedCustomers.count() // served from cache now

  customerAll.cache()
  customerAll.createOrReplaceTempView("customers_cached")
  customerAll.count()
  val storage = spark.sparkContext.getRDDStorageInfo
  storage.foreach(rdd => println(s"cached rdd: ${rdd.numPartitions} partitions, ${rdd.memSize} bytes mem"))
  spark.catalog.clearCache()

  // a Dataset is a DataFrame under the hood, so cache() behaves identically
  orders.as[Order].cache()
  orders.as[Order].count()


  /**
   * Persist levels: MEMORY_ONLY, MEMORY_ONLY_SER, MEMORY_AND_DISK,
   * MEMORY_AND_DISK_SER, DISK_ONLY. Serialized levels trade CPU for memory.
   */
  customerAll.rdd.persist(StorageLevel.DISK_ONLY).count()
  customerAll.unpersist()

  /**
   * Checkpointing — truncates the lineage so very long DAGs don't explode
   * recomputation cost when a node fails. Requires a directory.
   */
  spark.sparkContext.setCheckpointDir("target/checkpoint")
  val checkpointed = customerAll.rdd.map(row => row).checkpoint()
  checkpointed.count()
  println("checkpoint location: " + checkpointed.getCheckpointFile)


  /**
   * Join optimizations — broadcast joins for small tables vs SortMergeJoin.
   */
  val lookup: Map[String, String] = Map("hyd" -> "Telangana", "Bangalore" -> "Karnataka")
  val stateLookup = spark.createDataFrame(lookup.toSeq).toDF("city", "state")

  // sql (temp view) — hint forces a broadcast
  stateLookup.createOrReplaceTempView("states")
  spark.sql("SELECT /*+ BROADCAST(s) */ c.cust_name, s.state FROM customers c JOIN states s ON c.city = s.city").explain("extended")

  // dataframe — BROADCAST join hint
  customerAll.join(stateLookup.hint("broadcast"), "city").explain("extended")

  // Also possible: sort-merge vs shuffle-hash join hints
  customerAll.hint("shuffle_hash").join(orders, "cust_id").explain("extended")

  // rdd — the "broadcast join" approximation without hints
  val stateBc = spark.sparkContext.broadcast(stateLookup.collect().map(r => r.getString(0) -> r.getString(1)).toMap)
  customerAll.rdd
    .map(row => (row.getAs[String]("cust_name"), stateBc.value.getOrElse(row.getAs[String]("city"), "NA")))
    .take(5)
    .foreach(println)

  // dataset — Dataset API uses the same Catalyst plan as DataFrames
  customerAll.as[Customer].joinWith(stateLookup.as[StateRow].hint("broadcast"), customerAll("city") === stateLookup("city"))
    .explain("extended")


  /**
   * Coalescing/repartition before heavy ops to control parallelism.
   */
  val big = spark.range(0, 1000000, 1, 64)
  println("logical partitions    : " + big.rdd.getNumPartitions)
  val staged = big.repartition(16)
  println("after repartition(16) : " + staged.rdd.getNumPartitions)
  staged.coalesce(4).cache().count()
  println("after coalesce(4)     : " + big.coalesce(4).rdd.getNumPartitions)


  /**
   * Skew handling: salting the join key. Add a random bucket to the
   * hot key(s) on both sides so the shuffle spreads them across tasks.
   */
  import org.apache.spark.sql.types._
  val salty = orders.withColumn("cust_id", col("cust_id").cast("long"))
    .withColumn("salt", (rand() * 4).cast(IntegerType))
    .withColumn("salted_key", struct(col("cust_id"), col("salt")))
  val saltyCustomers = customerAll
    .withColumn("salt", (rand() * 4).cast(IntegerType))
    .withColumn("salted_key", struct(col("cust_id"), col("salt")))

  saltyCustomers.join(salty, Seq("salted_key"), "inner").explain("extended")
  saltyCustomers.join(salty, Seq("salted_key"), "inner").show(false)

  spark.stop()
}