package basics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import datasets._

object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("01-sql-basics")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)

  customerAll.createOrReplaceTempView("customers")

  /**
   * Show all columns from the customers dataset.
   */
  // sql (temp view)
  spark.sql("SELECT * FROM customers").show(false)

  // rdd
  customerAll.rdd.foreach(println)

  // dataframe
  customerAll.show(false)

  // dataset
  customerAll.as[Customer].show(false)


  /**
   * Show only cust_id, cust_name, and city.
   */
  // sql (temp view)
  spark.sql("SELECT cust_id, cust_name, city FROM customers").show(false)

  // rdd
  customerAll.rdd
    .map(row => (row.getAs[Long]("cust_id"), row.getAs[String]("cust_name"), row.getAs[String]("city")))
    .foreach(println)

  // dataframe
  customerAll.select("cust_id", "cust_name", "city").show(false)

  // dataset
  customerAll.as[Customer]
    .map(c => (c.cust_id, c.cust_name, c.city))
    .show(false)


  /**
   * Find the database currently connected / in use.
   *
   * This is catalog metadata rather than a row transformation, so the
   * rdd and dataset sections below don't apply in the usual sense —
   * Spark's RDD/Dataset APIs operate on data, not on session catalog state.
   */
  // sql (temp view)
  spark.sql("SELECT current_database()").show(false)

  // rdd
  // not applicable — no row-level RDD equivalent for catalog/session state

  // dataframe
  println(spark.catalog.currentDatabase)

  // dataset
  // not applicable — same reasoning as the rdd section above


  /**
   * List the tables and inspect the structure of customers.
   */
  // sql (temp view)
  spark.sql("SHOW TABLES").show(false)
  spark.sql("DESCRIBE customers").show(false)

  // rdd
  // not applicable — schema/catalog inspection isn't an RDD row operation

  // dataframe
  spark.catalog.listTables().show(false)
  customerAll.printSchema()

  // dataset
  // not applicable — same reasoning as the rdd section above


  /**
   * Count the number of rows in customers.
   */
  // sql (temp view)
  spark.sql("SELECT COUNT(*) FROM customers").show(false)

  // rdd
  customerAll.rdd.count()

  // dataframe
  customerAll.count()

  // dataset
  customerAll.as[Customer].count()

  spark.stop()
}
