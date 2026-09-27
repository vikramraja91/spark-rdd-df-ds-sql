package nullHandling

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import datasets._

case class CityRow(id: Int, name: String, city: Option[String])
case class ScoreRow(id: Int, score: Option[Double])
case class LeftRow(k: Int, lv: String)
case class RightRow(k: Int, rv: String)

object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("11-null-handling")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val (customerAll, orders, customerUpdates) = PracticeData.load(spark)

  val withNulls = Seq(
    (1, "A", Option("hyd")),
    (2, "B", Option.empty[String]),
    (3, "C", Option("chennai")),
    (4, "D", Option.empty[String])
  ).toDF("id", "name", "city")
   .withColumn("city", col("city").cast("string"))

  withNulls.createOrReplaceTempView("nulls")

  /**
   * Direct null checks: IS NULL / IS NOT NULL.
   */
  // sql (temp view)
  spark.sql("SELECT * FROM nulls WHERE city IS NULL").show(false)
  spark.sql("SELECT * FROM nulls WHERE city IS NOT NULL").show(false)

  // rdd
  withNulls.rdd.filter(row => row.getAs[String]("city") == null).foreach(println)
  withNulls.rdd.filter(row => row.getAs[String]("city") != null).foreach(println)

  // dataframe
  withNulls.filter(col("city").isNull).show(false)
  withNulls.filter(col("city").isNotNull).show(false)

  // dataset
  withNulls.as[CityRow].filter(_.city.isEmpty).show(false)
  withNulls.as[CityRow].filter(_.city.isDefined).show(false)


  /**
   * Replace nulls with a fallback: COALESCE / NVL.
   */
  // sql (temp view)
  spark.sql("SELECT id, name, COALESCE(city, 'unknown') AS city_filled, NVL(city, 'unknown') AS city_nvl FROM nulls").show(false)

  // rdd
  withNulls.rdd.map(row => (row.getAs[Int]("id"), Option(row.getAs[String]("city")).getOrElse("unknown"))).foreach(println)

  // dataframe
  withNulls.select(col("id"), coalesce(col("city"), lit("unknown")).alias("city_filled")).show(false)

  // dataset — Option.map/getOrElse is the natural typed way
  withNulls.as[CityRow]
    .map(c => (c.id, c.city.getOrElse("unknown")))
    .show(false)


  /**
   * Aggregate null semantics: AVG/SUM ignore nulls, COUNT(column) skips
   * them, COUNT(*) counts them.
   */
  val withScores = Seq(
    (1, Option(10.0)),
    (2, Option.empty[Double]),
    (3, Option(30.0))
  ).toDF("id", "score")
  withScores.createOrReplaceTempView("scores")

  // sql (temp view)
  spark.sql("""
    SELECT COUNT(*)          AS row_count,
           COUNT(score)      AS non_null_scores,
           SUM(score)        AS sum_ignoring_null,
           AVG(score)        AS avg_ignoring_null
    FROM scores
  """).show(false)

  // rdd
  val scores = withScores.rdd.map(row => Option(row.getAs[Double]("score")))
  println((scores.count(), scores.flatten.count(), scores.flatten.sum, scores.flatten.sum / scores.flatten.count))

  // dataframe
  withScores.select(count("*").alias("row_count"),
    count("score").alias("non_null_scores"),
    sum("score").alias("sum_ignoring_null"),
    avg("score").alias("avg_ignoring_null")).show(false)

  // dataset
  withScores.as[ScoreRow]
    .select(count("*").alias("row_count"),
      sum("score").alias("sum_ignoring_null"))
    .show(false)


  /**
   * df.na.fill and df.na.drop for DataFrame-level null cleanup.
   */
  // rdd — manual; there is no DataFrame-style na convenience on RDDs
  withNulls.rdd
    .map(row => (row.getAs[Int]("id"), Option(row.getAs[String]("city")).getOrElse("N/A")))
    .foreach(println)

  // dataframe
  withNulls.na.fill("unknown", Seq("city")).show(false)
  withNulls.na.drop("any").show(false)          // drop any row with a null
  withNulls.na.drop("all").show(false)          // drop rows where ALL fields are null

  // dataset
  withNulls.as[CityRow]
    .map(c => (c.id, c.name, c.city.getOrElse("N/A")))
    .show(false)


  /**
   * Join null semantics — null keys never match each other by default.
   */
  val left = Seq((1, "a"), (2, "b")).toDF("k", "lv")
  val right = Seq((1, "x"), (2, "y"), (null, "z")).toDF("k", "rv")

  // sql (temp view)
  left.createOrReplaceTempView("l"); right.createOrReplaceTempView("r")
  spark.sql("SELECT * FROM l INNER JOIN r ON l.k = r.k").show(false)

  // rdd — null keys drop out of the join unless you normalize them first
  left.rdd
    .map(row => (row.getAs[Int]("k"), row.getAs[String]("lv")))
    .join(right.rdd.map(row => (row.getAs[Int]("k"), row.getAs[String]("rv"))))
    .foreach(println)

  // dataframe
  left.join(right, "k").show(false)

  // dataset
  left.as[LeftRow]
    .joinWith(right.as[RightRow], left("k") === right("k"))
    .show(false)

  spark.stop()
}