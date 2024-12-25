package com.jay.interview

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object SplitRecordsNullEmpty {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("JSON_Sample").master("local[1]") getOrCreate()

    val argStr = Array("Hello", "world", "it's", "me")
    println(argStr.mkString(" "))
    val data = Seq("1, jaya  bothell wa",
    "2, Prem, Ji  bothell wa",
    "3, ,  Seattle wa")

    val rdd = spark.sparkContext.parallelize(data)
    import spark.implicits._
    val df = rdd.toDF("raw_data")

    df.show(false)

    val df1= df.withColumn("split_data", expr("split(raw_data, ',') ") )
    df1.select($"split_data".getItem(0).alias("id"),
      $"split_data".getItem(1).alias("name"),
      $"split_data".getItem(2).alias("location")).show(false)

  }
}
