package com.jay.file

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.sum

object WindowFunction_CSV {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark =SparkSession.builder().appName("Aggregation").config("spark.sql.warehouse.dir", "/file:C:/temp")
      .master("local")
      .getOrCreate()
    val txtfile = "./src/main/resources/DepartmentTable.csv"

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
