package datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Case classes used by the Dataset-style solutions across every module.
// Load these together with PracticeData.load(spark) before running a module's SparkPractice.scala.
case class Customer(cust_id: Long, cust_name: String, city: String, signup_date: java.sql.Date)
case class Order(order_id: Int, cust_id: Int, order_date: java.sql.Date, amount: Double, status: String)
case class CustomerUpdate(record_id: Int, cust_id: Int, cust_name: String, city: String, updated_at: java.sql.Timestamp, operation: String)

object PracticeData {
  def load(spark: SparkSession) = {
    import spark.implicits._

    val customers = Seq(
      (1, "vikram", "hyd", "2024-01-10"),
      (2, "Rahul", "Bangalore", "2024-02-15"),
      (3, "Priya", "Chennai", "2024-03-20")
    ).toDF("cust_id", "cust_name", "city", "signup_date")
      .withColumn("signup_date", col("signup_date").cast("date"))

    val custGenerated =
      spark.range(24, 1001)
        .withColumnRenamed("id", "cust_id")
        .withColumn("cust_name", concat(lit("Customer_"), col("cust_id")))
        .withColumn(
          "city",
          when(col("cust_id") % 5 === 0, "hyd")
            .when(col("cust_id") % 5 === 1, "Bangalore")
            .when(col("cust_id") % 5 === 2, "Chennai")
            .when(col("cust_id") % 5 === 3, "Mumbai")
            .otherwise("Delhi")
        )
        .withColumn(
          "signup_date",
          date_add(
            lit("2024-01-01").cast("date"),
            (col("cust_id") % 365).cast("int")
          )
        )

    val customerAll = customers.unionByName(custGenerated)

    val orders = Seq(
      (101, 1, "2025-01-10", 500.00, "Completed"),
      (102, 2, "2025-01-11", 750.00, "Completed"),
      (103, 1, "2025-01-15", 300.00, "Pending"),
      (104, 5, "2025-02-01", 1200.00, "Completed"),
      (105, 10, "2025-02-10", 450.00, "Cancelled"),
      (106, 500, "2025-02-15", 900.00, "Completed"),
      (107, 700, "2025-03-01", 1500.00, "Completed")
    ).toDF("order_id", "cust_id", "order_date", "amount", "status")
      .withColumn("order_date", col("order_date").cast("date"))

    val customerUpdates = Seq(
      (1, 1, "vikram", "hyd", "2025-01-01 10:00:00", "INSERT"),
      (2, 1, "vikram", "Bangalore", "2025-02-01 10:00:00", "UPDATE"),
      (3, 1, "vikram", "Chennai", "2025-03-01 10:00:00", "UPDATE"),
      (4, 2, "Rahul", "Bangalore", "2025-01-10 10:00:00", "INSERT"),
      (5, 2, "Rahul", "Mumbai", "2025-02-15 10:00:00", "UPDATE"),
      (6, 3, "Priya", "Chennai", "2025-01-05 10:00:00", "INSERT")
    ).toDF("record_id", "cust_id", "cust_name", "city", "updated_at", "operation")
      .withColumn("updated_at", col("updated_at").cast("timestamp"))

    (customerAll, orders, customerUpdates)
  }
}
