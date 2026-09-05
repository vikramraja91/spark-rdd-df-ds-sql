package nestedStructTypes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

case class Address(city: String, zip: String)
case class Employee(emp_id: Int, name: String, address: Address, skills: Seq[String], scores: Map[String, Int])

object SparkPractice extends App {

  val spark = SparkSession.builder()
    .appName("08-nested-and-complex-types")
    .master("local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  /**
   * Way 1 — case classes + toDS(). The simplest way when you're writing
   * Scala directly: nesting a case class inside another just works.
   */
  val employeesWay1: Seq[Employee] = Seq(
    Employee(1, "Vikram", Address("hyd", "500001"), Seq("Scala", "Spark"), Map("Scala" -> 8, "Spark" -> 7)),
    Employee(2, "Priya", Address("Chennai", "600001"), Seq("SQL", "Python"), Map("SQL" -> 9, "Python" -> 6)),
    Employee(3, "Rahul", Address("Bangalore", "560001"), Seq("Spark", "Kafka"), Map("Spark" -> 8, "Kafka" -> 7))
  )
  val employeesDS1 = employeesWay1.toDS()
  employeesDS1.show(false)
  employeesDS1.printSchema()


  /**
   * Way 2 — explicit StructType schema + Row(Row(...), Seq(...), Map(...))
   * + createDataFrame. Useful when the schema is only known at runtime
   * (e.g. read from a config or inferred elsewhere).
   */
  val employeeSchema = StructType(Seq(
    StructField("emp_id", IntegerType, nullable = false),
    StructField("name", StringType, nullable = false),
    StructField("address", StructType(Seq(
      StructField("city", StringType, nullable = true),
      StructField("zip", StringType, nullable = true)
    )), nullable = true),
    StructField("skills", ArrayType(StringType), nullable = true),
    StructField("scores", MapType(StringType, IntegerType), nullable = true)
  ))

  val employeeRows = Seq(
    Row(1, "Vikram", Row("hyd", "500001"), Seq("Scala", "Spark"), Map("Scala" -> 8, "Spark" -> 7)),
    Row(2, "Priya", Row("Chennai", "600001"), Seq("SQL", "Python"), Map("SQL" -> 9, "Python" -> 6)),
    Row(3, "Rahul", Row("Bangalore", "560001"), Seq("Spark", "Kafka"), Map("Spark" -> 8, "Kafka" -> 7))
  )
  val employeesDF2 = spark.createDataFrame(
    spark.sparkContext.parallelize(employeeRows),
    employeeSchema
  )
  employeesDF2.show(false)

  val employees = employeesDS1.toDF()

  employees.createOrReplaceTempView("employees")

  /**
   * Extract the city from the nested address struct.
   */
  // sql (temp view)
  spark.sql("SELECT emp_id, name, address.city AS city FROM employees").show(false)

  // rdd
  employees.rdd
    .map(row => (row.getAs[Int]("emp_id"), row.getAs[String]("name"), row.getAs[Row]("address").getAs[String]("city")))
    .foreach(println)

  // dataframe
  employees.select(col("emp_id"), col("name"), col("address.city").alias("city")).show(false)

  // dataset
  employeesDS1.map(e => (e.emp_id, e.name, e.address.city)).show(false)


  /**
   * Flatten skills into one row per (emp_id, skill) — explode the array.
   */
  // sql (temp view)
  spark.sql("SELECT emp_id, name, skill FROM employees LATERAL VIEW explode(skills) AS skill").show(false)

  // rdd
  employees.rdd
    .flatMap(row =>
      row.getAs[Seq[String]]("skills").map(skill => (row.getAs[Int]("emp_id"), row.getAs[String]("name"), skill))
    )
    .foreach(println)

  // dataframe
  employees.select(col("emp_id"), col("name"), explode(col("skills")).alias("skill")).show(false)

  // dataset
  employeesDS1.flatMap(e => e.skills.map(skill => (e.emp_id, e.name, skill))).show(false)


  /**
   * Look up a specific skill's score from the scores map.
   */
  // sql (temp view)
  spark.sql("SELECT emp_id, name, scores['Spark'] AS spark_score FROM employees").show(false)

  // rdd
  employees.rdd
    .map(row => (row.getAs[Int]("emp_id"), row.getAs[String]("name"), row.getAs[Map[String, Int]]("scores").get("Spark")))
    .foreach(println)

  // dataframe
  employees.select(col("emp_id"), col("name"), col("scores").getItem("Spark").alias("spark_score")).show(false)

  // dataset
  employeesDS1.map(e => (e.emp_id, e.name, e.scores.get("Spark"))).show(false)


  /**
   * Filter employees who live in "hyd" (nested struct field) and know "Spark" (array).
   */
  // sql (temp view)
  spark.sql("""
    SELECT emp_id, name
    FROM employees
    WHERE address.city = 'hyd' AND array_contains(skills, 'Spark')
  """).show(false)

  // rdd
  employees.rdd
    .filter(row =>
      row.getAs[Row]("address").getAs[String]("city") == "hyd" &&
        row.getAs[Seq[String]]("skills").contains("Spark")
    )
    .map(row => (row.getAs[Int]("emp_id"), row.getAs[String]("name")))
    .foreach(println)

  // dataframe
  employees
    .filter(col("address.city") === "hyd" && array_contains(col("skills"), "Spark"))
    .select("emp_id", "name")
    .show(false)

  // dataset
  employeesDS1
    .filter(e => e.address.city == "hyd" && e.skills.contains("Spark"))
    .map(e => (e.emp_id, e.name))
    .show(false)


  /**
   * Compute the average score per employee across their scores map.
   */
  // sql (temp view) — explode the map, then aggregate
  spark.sql("""
    SELECT emp_id, name, AVG(score) AS avg_score
    FROM employees LATERAL VIEW explode(scores) AS skill, score
    GROUP BY emp_id, name
  """).show(false)

  // rdd
  employees.rdd
    .map { row =>
      val scores = row.getAs[Map[String, Int]]("scores").values
      (row.getAs[Int]("emp_id"), row.getAs[String]("name"), scores.sum.toDouble / scores.size)
    }
    .foreach(println)

  // dataframe
  employees
    .select(col("emp_id"), col("name"), explode(col("scores")).as(Seq("skill", "score")))
    .groupBy("emp_id", "name")
    .agg(avg("score").alias("avg_score"))
    .show(false)

  // dataset
  employeesDS1
    .map(e => (e.emp_id, e.name, e.scores.values.sum.toDouble / e.scores.values.size))
    .show(false)

  spark.stop()




}
