package com.jay.file

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ORC_FileRead {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("Aggregation").config("spark.sql.warehouse.dir", "/file:C:/temp")
      .master("local")
      .getOrCreate()
    val df = spark.read.format("orc").load("./src/main/resources/users.orc")
    df.printSchema()
    df.show()
  }
}
