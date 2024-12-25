package com.jay.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SortEmployeeTable {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder
      .appName("Sort Employee Table")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Sample data
    val employeeData = Seq(
      (1, "Alice", "2022-01-01", 50000),
      (2, "Bob", "2021-06-15", 60000),
      (3, "Charlie", "2023-03-20", 70000),
      (4, "David", "2020-12-05", 55000)
    )

    // Create DataFrame
    val employeeDF: DataFrame = employeeData.toDF("id", "name", "date", "salary")

    // Convert date column to DateType
    val employeeDFWithDate = employeeDF.withColumn("date", to_date($"date", "yyyy-MM-dd"))

    // Sort by salary in ascending order
    val sortedBySalaryDF = employeeDFWithDate.orderBy("salary")
    sortedBySalaryDF.show()

    // Sort by date in descending order
    val sortedByDateDescDF = employeeDFWithDate.orderBy($"date".desc)
    sortedByDateDescDF.show()

    // Sort by name in ascending order and then by salary in descending order
    val sortedByNameAndSalaryDF = employeeDFWithDate.orderBy($"name".asc, $"salary".desc)
    sortedByNameAndSalaryDF.show()

    // Stop SparkSession
    spark.stop()
  }
}
