package com.jay.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.first

object FirstLastValue {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("FirstLastWindowsFunction").getOrCreate()


    import spark.implicits._
    val rdd = Seq(
      (1, "a", 100),
      (2, "b", 200),
      (3, "c", 300),
      (4, "a", 400),
      (5, "b", 500),
      (6, "c", 600)
    ).toDF("id", "category", "value")

    rdd.show(false)

    val windowSpac = Window.partitionBy("category").orderBy("id")

    val result = rdd.withColumn("first_Value", first("value") over(windowSpac))
    result.show(false)
  }
}
