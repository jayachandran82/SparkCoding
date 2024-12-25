package com.jay.interview

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Input record:
----------------------------------------
User ID | Timestamp           | Page Visited
--------|---------------------|--------------
1       | 2024-02-01 08:00:00 | Home
1       | 2024-02-01 08:10:00 | Products
1       | 2024-02-01 08:35:00 | Checkout
2       | 2024-02-01 09:00:00 | Home
2       | 2024-02-01 09:05:00 | Products
1       | 2024-02-01 10:00:00 | Home


Expected Output:
---------------------------------------------------

User ID | Total Sessions | Average Session Duration (minutes)
--------|----------------|-------------------------------------
1       | 2              | 17.5
2       | 1              | 5

 */
object TotalAvgSession {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("TotalAvgSession").master("local[1]") getOrCreate()

    // Sample Data
    val data = Seq(
      (1, "2024-02-01 08:00:00", "Home"),
      (1, "2024-02-01 08:10:00", "Products"),
      (1, "2024-02-01 08:35:00", "Checkout"),
      (2, "2024-02-01 09:00:00", "Home"),
      (2, "2024-02-01 09:05:00", "Products"),
      (1, "2024-02-01 10:00:00", "Home")
    )

    // Convert Data to DataFrame using toDF()
    import spark.implicits._
    val df = data.toDF("UserID", "Timestamp", "PageVisited")
      .withColumn("Timestamp", col("Timestamp").cast(TimestampType))

    df.printSchema()
    df.show(false)

    // Window Spec to Partition and Order by UserID and Timestamp
    val windowSpec = Window.partitionBy("UserID").orderBy("Timestamp")


    // Calculate the difference between consecutive timestamps
    val sessionizedDF = df.withColumn("PreviousTimestamp", lag("Timestamp", 1).over(windowSpec))
      .withColumn("TimeDiff", (unix_timestamp(col("Timestamp")) - unix_timestamp(col("PreviousTimestamp"))) / 60)
      .withColumn("NewSession", when(col("TimeDiff").isNull || col("TimeDiff") > 30, 1).otherwise(0))

    println("Calculate the difference between consecutive timestamps")
    sessionizedDF.show(false)

    // Assign session ID to each record
    val sessionDF = sessionizedDF.withColumn("SessionID", sum("NewSession").over(windowSpec))
    println("Assign session ID to each record")
    sessionDF.show(false)

    // Calculate session duration
    val sessionDurationDF = sessionDF.groupBy("UserID", "SessionID")
      .agg((unix_timestamp(max("Timestamp")) - unix_timestamp(min("Timestamp"))).as("SessionDuration"))
    println("Calculate session duration")
    sessionDurationDF.show(false)

    // Calculate total sessions and average session duration
    val resultDF = sessionDurationDF.groupBy("UserID")
      .agg(
        count("SessionID").as("TotalSessions"),
        avg("SessionDuration").as("AverageSessionDuration")
      )

    println("Calculate total sessions and average session duration")
    resultDF.show(false)
    // Convert average session duration from seconds to minutes
    val finalDF = resultDF.withColumn("AverageSessionDuration", round(col("AverageSessionDuration") / 60, 1))

    // Show the result
    println("Show the result")
    finalDF.show(false)
  }

}
