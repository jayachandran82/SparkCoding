package com.jay.sum

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ThreeDaySum {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("ThreeDaySum ")
      .master("local[2]").getOrCreate()


    import spark.implicits._

    val data = Seq(
      ("2024-07-25", 3000),
      ("2024-07-26", 8000),
      ("2024-07-27", 5000),
      ("2024-07-28", 9000),
      ("2024-07-29", 3000),
      ("2024-07-30", 8000),
      ("2024-07-31", 12000),
      ("2024-08-01", 9000)
    ).toDF("date", "amount")

    val windowSpec = Window.orderBy($"date".cast("timestamp")).rowsBetween(-2, 0)

    val result = data.withColumn("3_day_sum", sum($"amount").over(windowSpec))

    result.show()
  }
}
