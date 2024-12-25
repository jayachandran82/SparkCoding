package com.jay.file

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Parquet_File {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("parquet")
      .master("local[2]")
      .getOrCreate()
    val df = spark.read.load("./src/main/resources/users.parquet")
    df.printSchema()
    df.show()
  }

}
