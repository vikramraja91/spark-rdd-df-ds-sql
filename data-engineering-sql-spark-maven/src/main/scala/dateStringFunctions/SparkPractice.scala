package dateStringFunctions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import datasets._

object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("13-date-and-string-functions")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)
  customerAll.createOrReplaceTempView("customers")
  orders.createOrReplaceTempView("orders")
  customerUpdates.createOrReplaceTempView("customer_updates")

  val customersDs = customerAll.as[Customer]
  val ordersDs = orders.as[Order]
  val updatesDs = customerUpdates.as[CustomerUpdate]

  /**
   * Date functions: extract parts, diff dates, format, back-and-forth casts.
   */
  // sql (temp view)
  spark.sql("""
    SELECT cust_id,
           YEAR(signup_date)   AS yr,
           MONTH(signup_date)  AS mth,
           DAYOFMONTH(signup_date) AS day,
           DATEDIFF(CURRENT_DATE(), signup_date)  AS days_since_signup,
           DATE_FORMAT(signup_date, 'yyyy-MM')    AS ym
    FROM customers
    ORDER BY cust_id
    LIMIT 5
  """).show(false)

  // rdd
  val d = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")
  customerAll.rdd
    .map(row => (row.getAs[Long]("cust_id"), row.getAs[java.sql.Date]("signup_date")))
    .map { case (id, date) => (id, date.toLocalDate.getYear, date.toLocalDate.getMonthValue, d.format(date.toLocalDate) ) }
    .take(5)
    .foreach(println)

  // dataframe
  customersDs.toDF()
    .withColumn("yr", year(col("signup_date")))
    .withColumn("days_since_signup", datediff(current_date(), col("signup_date")))
    .withColumn("ym", date_format(col("signup_date"), "yyyy-MM"))
    .select("cust_id", "yr", "days_since_signup", "ym")
    .show(false)

  // dataset
  customersDs
    .map(c => (c.cust_id, c.signup_date.toLocalDate.getYear, java.time.temporal.ChronoUnit.DAYS.between(c.signup_date.toLocalDate, java.time.LocalDate.now())))
    .show(false)


  /**
   * Parse strings into dates / timestamps.
   */
  val rawDates = Seq("2025-01-01", "2025-02-14", "02/28/2025").toDF("value")
  rawDates.createOrReplaceTempView("raw_dates")

  // sql (temp view)
  spark.sql("SELECT value, TO_DATE(value, 'MM/dd/yyyy') AS parsed FROM raw_dates").show(false)

  // rdd
  val fmt = java.time.format.DateTimeFormatter.ofPattern("MM/dd/yyyy")
  rawDates.rdd
    .map(row => java.time.LocalDate.parse(row.getAs[String]("value"), fmt))
    .foreach(println)

  // dataframe
  rawDates.select(to_date(col("value"), "MM/dd/yyyy").alias("parsed")).show(false)

  // dataset
  rawDates.as[String].map(s => java.time.LocalDate.parse(s, fmt)).show(false)


  /**
   * String functions: case, trimming, splitting, substring, regex, concat.
   */
  val rawNames = Seq("  vikram  ", "Rahul", "PRIYA", "anil patel", "customer@test.com").toDF("value")
  rawNames.createOrReplaceTempView("names")

  // sql (temp view)
  spark.sql("""
    SELECT value,
           TRIM(value)                AS trimmed,
           UPPER(value)               AS upper_cased,
           LOWER(value)               AS lower_cased,
           SUBSTRING(value, 1, 3)     AS first_three,
           SPLIT(value, ' ')          AS parts
    FROM names
  """).show(false)

  // rdd
  rawNames.rdd
    .map(row => row.getAs[String]("value").trim.toUpperCase)
    .foreach(println)

  // dataframe
  rawNames.select(
    trim(col("value")).alias("trimmed"),
    upper(col("value")).alias("upper_cased"),
    split(col("value"), " +").alias("parts"),
    regexp_extract(col("value"), "([\\w.]+)@", 1).alias("user_at")
  ).show(false)

  // dataset
  rawNames.as[String]
    .map { n =>
      val trimmed = n.trim
      val first = trimmed.take(3)
      val split = trimmed.split("\\s+").toSeq
      (first, split)
    }
    .show(false)


  /**
   * Regex replacement and extraction on a log-ish column.
   */
  val logs = Seq(
    "2025-01-05 10:00:00 INFO  user=1 action=login",
    "2025-01-05 10:02:00 ERROR user=2 action=checkout",
    "2025-01-05 10:03:00 INFO  user=3 action=login"
  ).toDF("value")
  logs.createOrReplaceTempView("logs")

  // sql (temp view)
  spark.sql("""
    SELECT value,
           REGEXP_EXTRACT(value, 'user=(\\d+)', 1) AS user_id,
           REGEXP_EXTRACT(value, 'action=(\\w+)', 1) AS action,
           REGEXP_REPLACE(value, '\\d{4}-\\d{2}-\\d{2}', 'REDACTED') AS masked
    FROM logs
  """).show(false)

  // rdd
  val userPattern = "user=(\\d+)".r
  logs.rdd
    .map(row => userPattern.findFirstMatchIn(row.getAs[String]("value")).map(_.group(1)))
    .foreach(println)

  // dataframe
  logs.select(
    regexp_extract(col("value"), "user=(\\d+)", 1).alias("user_id"),
    regexp_extract(col("value"), "action=(\\w+)", 1).alias("action")
  ).show(false)

  // dataset
  logs.as[String]
    .map { log => userPattern.findFirstMatchIn(log).map(_.group(1)) }
    .show(false)

  spark.stop()
}