package filteringAndSorting

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import datasets._

/**
 * Run with: mvn -q exec:java -Dexec.mainClass=module02.SparkPractice
 * (or just click the green run arrow on this object in IntelliJ)
 */
object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("02-filtering-and-sorting")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)
  customerAll.createOrReplaceTempView("customers")

  /**
   * Customers from Hyderabad.
   */
  // sql (temp view)
  spark.sql("SELECT * FROM customers WHERE city = 'hyd'").show(false)

  // rdd
  customerAll.rdd.filter(row => row.getAs[String]("city") == "hyd").foreach(println)

  // dataframe
  customerAll.filter(col("city") === "hyd").show(false)

  // dataset
  customerAll.as[Customer].filter(_.city == "hyd").show(false)


  /**
   * Customers from Hyderabad or Bangalore.
   */
  // sql (temp view)
  spark.sql("SELECT * FROM customers WHERE city = 'hyd' OR city = 'Bangalore'").show(false)

  // rdd
  customerAll.rdd
    .filter(row => Set("hyd", "Bangalore").contains(row.getAs[String]("city")))
    .foreach(println)

  // dataframe
  customerAll.filter(col("city") === "hyd" || col("city") === "Bangalore").show(false)

  // dataset
  customerAll.as[Customer].filter(c => c.city == "hyd" || c.city == "Bangalore").show(false)


  /**
   * Customer IDs between 200 and 500 (inclusive), showing only cust_name.
   */
  // sql (temp view) — BETWEEN is inclusive
  spark.sql("SELECT cust_name FROM customers WHERE cust_id BETWEEN 200 AND 500").show(false)

  // rdd
  customerAll.rdd
    .filter(row => row.getAs[Long]("cust_id") >= 200 && row.getAs[Long]("cust_id") <= 500)
    .map(row => row.getAs[String]("cust_name"))
    .foreach(println)

  // dataframe
  customerAll.filter(col("cust_id").between(200, 500)).select("cust_name").show(false)

  // dataset
  customerAll.as[Customer]
    .filter(c => c.cust_id >= 200 && c.cust_id <= 500)
    .map(_.cust_name)
    .show(false)


  /**
   * Customer names beginning with "Customer_1".
   */
  // sql (temp view)
  spark.sql("SELECT cust_name FROM customers WHERE cust_name LIKE 'Customer_1%'").show(false)

  // rdd
  customerAll.rdd
    .filter(row => row.getAs[String]("cust_name").startsWith("Customer_1"))
    .map(row => row.getAs[String]("cust_name"))
    .foreach(println)

  // dataframe
  customerAll.filter(col("cust_name").startsWith("Customer_1")).select("cust_name").show(false)

  // dataset
  customerAll.as[Customer]
    .filter(_.cust_name.startsWith("Customer_1"))
    .map(_.cust_name)
    .show(false)


  /**
   * Customers where city is NULL.
   */
  // sql (temp view)
  spark.sql("SELECT cust_name FROM customers WHERE city IS NULL").show(false)

  // rdd
  customerAll.rdd
    .filter(row => row.isNullAt(row.fieldIndex("city")))
    .map(row => row.getAs[String]("cust_name"))
    .foreach(println)

  // dataframe
  customerAll.filter(col("city").isNull).select("cust_name").show(false)

  // dataset
  customerAll.as[Customer]
    .filter(c => c.city == null)
    .map(_.cust_name)
    .show(false)


  /**
   * Return distinct cities.
   */
  // sql (temp view)
  spark.sql("SELECT DISTINCT city FROM customers").show(false)

  // rdd
  customerAll.rdd.map(row => row.getAs[String]("city")).distinct().foreach(println)

  // dataframe
  customerAll.select("city").distinct().show(false)

  // dataset
  customerAll.as[Customer].map(_.city).distinct().show(false)


  /**
   * Customers from hyd, Bangalore, or Chennai using IN.
   */
  // sql (temp view)
  spark.sql("SELECT cust_name FROM customers WHERE city IN ('hyd', 'Bangalore', 'Chennai')").show(false)

  // rdd
  val allowedCities = Set("hyd", "Bangalore", "Chennai")
  customerAll.rdd
    .filter(row => allowedCities.contains(row.getAs[String]("city")))
    .map(row => row.getAs[String]("cust_name"))
    .foreach(println)

  // dataframe
  customerAll.filter(col("city").isin("hyd", "Bangalore", "Chennai")).select("cust_name").show(false)

  // dataset
  customerAll.as[Customer]
    .filter(c => allowedCities.contains(c.city))
    .map(_.cust_name)
    .show(false)


  /**
   * Hyderabad customers with cust_id > 500.
   */
  // sql (temp view)
  spark.sql("SELECT cust_name FROM customers WHERE city = 'hyd' AND cust_id > 500").show(false)

  // rdd
  customerAll.rdd
    .filter(row => row.getAs[String]("city") == "hyd" && row.getAs[Long]("cust_id") > 500)
    .map(row => row.getAs[String]("cust_name"))
    .foreach(println)

  // dataframe
  customerAll
    .filter(col("city") === "hyd" && col("cust_id") > 500)
    .select("cust_name")
    .show(false)

  // dataset
  customerAll.as[Customer]
    .filter(c => c.city == "hyd" && c.cust_id > 500)
    .map(_.cust_name)
    .show(false)


  /**
   * Hyderabad customers ordered by signup date ascending.
   */
  // sql (temp view)
  spark.sql("SELECT cust_name FROM customers WHERE city = 'hyd' ORDER BY signup_date ASC").show(false)

  // rdd
  customerAll.rdd
    .filter(row => row.getAs[String]("city") == "hyd")
    .map(row => (row.getAs[java.sql.Date]("signup_date"), row.getAs[String]("cust_name")))
    .sortBy(_._1.getTime)
    .map(_._2)
    .foreach(println)

  // dataframe
  customerAll
    .filter(col("city") === "hyd")
    .orderBy(col("signup_date").asc)
    .select("cust_name")
    .show(false)

  // dataset
  customerAll.as[Customer]
    .filter(_.city == "hyd")
    .sort(col("signup_date").asc)
    .map(_.cust_name)
    .show(false)

  spark.stop()
}
