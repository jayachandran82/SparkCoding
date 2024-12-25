package com.jay.regexp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{col, regexp_replace, when}

object Regexp_CSV_File {
  def singleSpace(col: Column): Column = {
    regexp_replace(col, "_", " ")
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val file = "./src/main/resources/regexp/abc.csv"
    val spark = SparkSession.builder().appName("Regexp_CSV_File").master("local[1]") getOrCreate()
    import spark.implicits._
    val data = spark
      .read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("charset", "UTF-8")
      .load(file)
    data.printSchema()
    data.toDF().show()

    data.withColumn("phone", when($"phone".isNull, 0).otherwise(col("Phone")))
      .withColumn("name", singleSpace($"name"))
      .show()
  }
}
