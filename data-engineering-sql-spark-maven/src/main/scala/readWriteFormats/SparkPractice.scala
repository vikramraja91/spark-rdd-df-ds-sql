package readWriteFormats

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import datasets._

// Typed projection matching the 3-column JSON/bucketing outputs.
case class PersonView(cust_id: Long, cust_name: String, city: String)

object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("10-read-write-formats")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)
  customerAll.createOrReplaceTempView("customers")

  // Output roots — a `target/data-output/<format>` dir per format
  val outRoot = "target/data-output"
  val customers = customerAll.select("cust_id", "cust_name", "city")

  /**
   * JSON — schema inference and write.
   */
  customers.write.mode(SaveMode.Overwrite).json(s"$outRoot/json")
  val jsonDf: DataFrame = spark.read.json(s"$outRoot/json")
  jsonDf.printSchema()
  jsonDf.show(false)

  // rdd — raw text read, no schema; the "format"-ish RDD read is just textFile
  spark.sparkContext.textFile(s"$outRoot/json").take(3).foreach(println)

  // sql (temp view)
  jsonDf.createOrReplaceTempView("json_customers")
  spark.sql("SELECT * FROM json_customers").show(false)

  // dataframe
  jsonDf.select("cust_id", "cust_name", "city").show(false)

  // dataset
  jsonDf.as[PersonView].map(p => (p.cust_id, p.cust_name)).show(false)


  /**
   * CSV — inferSchema + header options on read, header on write.
   */
  customerAll
    .withColumn("city", coalesce(col("city"), lit("")))
    .write.mode(SaveMode.Overwrite)
    .option("header", "true")
    .csv(s"$outRoot/csv")

  val csvDf: DataFrame = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(s"$outRoot/csv")
  csvDf.printSchema()
  csvDf.show(false)

  // rdd — CSV without a header is just text lines
  spark.sparkContext.textFile(s"$outRoot/csv").take(3).foreach(println)

  // explicit schema read (SDF: always do this in production, never infer on prod schedules)
  val explicitSchema = "cust_id LONG, cust_name STRING, city STRING, signup_date DATE"
  val csvTyped: DataFrame = spark.read
    .option("header", "true")
    .schema(explicitSchema)
    .csv(s"$outRoot/csv")
  csvTyped.show(false)

  // dataframe / sql — same as jsonDf pattern
  csvDf.select("cust_id", "cust_name").show(false)
  csvDf.createOrReplaceTempView("csv_customers")
  spark.sql("SELECT COUNT(*) FROM csv_customers").show(false)

  // dataset
  csvTyped.as[Customer].show(false)


  /**
   * Parquet — the default columnar format: compresses well, stores schema,
   * supports predicate pushdown. pre-split columns before writing (optional).
   */
  customerAll.withColumn("partition_key", lit("p1"))
    .write.mode(SaveMode.Overwrite)
    .partitionBy("partition_key")
    .parquet(s"$outRoot/parquet")

  val parquetDf: DataFrame = spark.read.parquet(s"$outRoot/parquet")
  parquetDf.show(false)
  parquetDf.printSchema()

  // rdd — no direct RDD analog; parquet reads go through the DataFrame API
  println("partitions on disk: " + new java.io.File(s"$outRoot/parquet").list().toSeq.filter(_.contains("partition_key")))

  // predicate pushdown is visible in the physical plan
  parquetDf.createOrReplaceTempView("parquet_customers")
  spark.sql("SELECT * FROM parquet_customers WHERE cust_id > 500").explain("extended")
  spark.sql("SELECT * FROM parquet_customers WHERE cust_id > 500").show(false)

  // dataframe / dataset
  parquetDf.filter(col("cust_id") > 500).show(false)
  parquetDf.select("cust_id", "cust_name", "city", "signup_date")
    .as[Customer]
    .filter(_.cust_id > 500)
    .show(false)


  /**
   * ORC — same columnar benefits as Parquet, ACID-compatible with Hive.
   */
  customerAll.select("cust_id", "cust_name", "city")
    .write.mode(SaveMode.Overwrite).orc(s"$outRoot/orc")
  val orcDf: DataFrame = spark.read.orc(s"$outRoot/orc")
  orcDf.show(false)


  /**
   * Bucketing — pre-group rows into buckets so later joins/aggregations
   * can skip shuffles entirely.
   */
  customerAll.as[Customer]
    .write.mode(SaveMode.Overwrite)
    .bucketBy(4, "city")
    .sortBy("cust_name")
    .saveAsTable("bucketed_customers")

  // the bucketed table is registered in the catalog automatically
  spark.sql("DESCRIBE EXTENDED bucketed_customers").show(false)
  spark.table("bucketed_customers").show(false)
  spark.sql("SHOW TABLES").show(false)
  spark.sql("DROP TABLE IF EXISTS bucketed_customers")

  /**
   * Writing many small outputs vs one — coalesce/repartition before write.
   */
  customers.repartition(4).write.mode(SaveMode.Overwrite).json(s"$outRoot/json-4parts")
  val writtenFiles = new java.io.File(s"$outRoot/json-4parts").list().toSeq.filter(!_.endsWith("crc"))
  println("files written with repartition(4): " + writtenFiles)

  customers.coalesce(1).write.mode(SaveMode.Overwrite).json(s"$outRoot/json-1part")
  val singleFile = new java.io.File(s"$outRoot/json-1part").list().toSeq.filter(_.startsWith("part-"))
  println("files written with coalesce(1): " + singleFile)

  spark.stop()
}