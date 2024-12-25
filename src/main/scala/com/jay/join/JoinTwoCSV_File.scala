package com.jay.join

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object JoinTwoCSV_File {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("JoinTwoCSV_File")
      .master("local[2]").getOrCreate()

    val empPay = spark.read.format("csv").option("header", "true")
      .option("charset", "UTF8").load("src/main/resources/join/employee_pay.csv")

    val empName = spark.read.format("csv").option("header", "true")
      .option("charset", "UTF8").load("src/main/resources/join/employee_names.csv")

    empPay.show()
    empName.show()
    empPay.join(empName, empPay("id") === empName("id"), "inner").show()
    empPay.join(empName, empPay("id") === empName("id"), "outer").show()

  }
}
