package udfsAndHigherOrder

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class Sales(region: String, units: Int, price: Double)
case class ItemsRow(id: Int, region: String, items: Seq[Double])

object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("09-udfs-and-higher-order-functions")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  val sales = Seq(
    Sales("east", 5, 10.0),
    Sales("east", 3, 20.0),
    Sales("west", 4, 15.0),
    Sales("north", 10, 5.0)
  ).toDS()
  sales.createOrReplaceTempView("sales")

  /**
   * User-defined functions. Prefer Spark's built-ins; UDFs are opaque to
   * the optimizer (e.g. no predicate pushdown, no codegen fusion).
   */
  val revenueUdf = udf((units: Int, price: Double) => units * price)
  spark.udf.register("revenue_udf", revenueUdf)

  // sql (temp view)
  spark.sql("SELECT region, revenue_udf(units, price) AS revenue FROM sales").show(false)

  // rdd
  sales.toDF().rdd.map(row => row.getAs[String]("region") -> (row.getAs[Int]("units") * row.getAs[Double]("price"))).foreach(println)

  // dataframe
  sales.select(col("region"), revenueUdf(col("units"), col("price")).alias("revenue")).show(false)

  // dataset — plain Scala function, no UDF wrapper needed
  sales.map(s => (s.region, s.units * s.price)).show(false)


  /**
   * Reducing the cost of UDFs: register with RETURNS for null semantics,
   * and prefer an expression (column) based approach when possible.
   */
  val asExpression = (col("units") * col("price")).alias("revenue_expr")
  sales.select(col("region"), asExpression).show(false)

  /**
   * UDAF — user-defined aggregate over a group of rows.
   */
  class MeanUDAF(zero: Double) extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = StructType(Seq(StructField("value", DoubleType)))
    override def bufferSchema: StructType = StructType(Seq(StructField("sum", DoubleType), StructField("count", LongType)))
    override def dataType: DataType = DoubleType
    override def deterministic: Boolean = true
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0.0
      buffer(1) = 0L
    }
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getDouble(0) + input.getDouble(0)
        buffer(1) = buffer.getLong(1) + 1L
      }
    }
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
    override def evaluate(buffer: Row): Double =
      if (buffer.getLong(1) == 0) zero else buffer.getDouble(0) / buffer.getLong(1)
  }

  val meanUda = new MeanUDAF(-1)
  spark.udf.register("mean_udaf", meanUda)

  // sql (temp view)
  spark.sql("SELECT region, mean_udaf(price) AS avg_price FROM sales GROUP BY region").show(false)

  // rdd
  sales.toDF().rdd
    .map(row => (row.getAs[String]("region"), (row.getAs[Double]("price"), 1)))
    .reduceByKey { case ((s1, c1), (s2, c2)) => (s1 + s2, c1 + c2) }
    .mapValues { case (sum, cnt) => sum / cnt }
    .foreach(println)

  // dataframe
  sales.groupBy("region").agg(meanUda(col("price")).alias("avg_price")).show(false)

  // dataset — typed aggregate with groupByKey + mapGroups
  sales.groupByKey(_.region)
    .mapGroups { case (region, rows) =>
      val prices = rows.map(_.price).toSeq
      (region, prices.sum / prices.size)
    }
    .show(false)


  /**
   * Higher-order functions (Spark 3): transform, filter, exists, reduce
   * applied inside SQL without unnesting the array.
   */
  val salesWithItems = Seq(
    (1, "east", Seq(1.0, 2.0, 3.0)),
    (2, "west", Seq(4.0, 5.0))
  ).toDF("id", "region", "items")

  salesWithItems.createOrReplaceTempView("with_items")

  // sql (temp view)
  spark.sql("""
    SELECT id,
           transform(items, x -> x * 2)                  AS doubled,
           filter(items, x -> x > 2)                     AS big,
           exists(items, x -> x > 4)                     AS any_big,
           aggregate(items, 0.0, (acc, x) -> acc + x)    AS total
    FROM with_items
  """).show(false)

  // rdd
  salesWithItems.rdd
    .map(row => row.getAs[Int]("id") -> (row.getAs[Seq[Double]]("items"), row.getAs[Seq[Double]]("items").exists(_ > 4)))
    .foreach(println)

  // dataframe
  salesWithItems.select(
    col("id"),
    transform(col("items"), x => x * 2).alias("doubled"),
    filter(col("items"), x => x > 2).alias("big"),
    exists(col("items"), x => x > 4).alias("any_big")
  ).show(false)

  // dataset
  salesWithItems.as[ItemsRow]
    .map(r => (r.id, r.items.map(_ * 2), r.items.filter(_ > 2), r.items.exists(_ > 4)))
    .show(false)

  spark.stop()
}