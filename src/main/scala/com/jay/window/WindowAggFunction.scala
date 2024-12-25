package com.jay.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum

object WindowAggFunction {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("WindowAggFunction")
      .master("local")
      .getOrCreate()
    val txtfile = "./src/main/resources/window/AggregationTable.txt"

    val empdata = spark.read
      .format("csv")
      .option("delimiter", " ")
      .option("header", "true")
      //.option("charset", "UTF8")
      .load(txtfile)
    empdata.printSchema()
    empdata.show()
    val byDepName = Window.partitionBy("DepName") // need to partition DepName
    empdata.withColumn("avg", sum("salary") over byDepName).show
  }
}
