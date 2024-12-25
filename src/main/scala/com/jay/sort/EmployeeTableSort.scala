package com.jay.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object EmployeeTableSort extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder
    .appName("Employee Table Sort")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val employees = Seq(
    (1, "John", "2022-01-01", 50000),
    (2, "Jane", "2022-02-01", 60000),
    (3, "Doe", "2022-01-15", 55000),
    (4, "Alice", "2022-03-01", 65000)
  ).toDF("id", "name", "date", "salary")

  // Sort by id
  val sortedById = employees.sort("id")
  sortedById.show()

  // Sort by name
  val sortedByName = employees.sort("name")
  sortedByName.show()

  // Sort by date
  val sortedByDate = employees.sort("date")
  sortedByDate.show()

  // Sort by salary
  val sortedBySalary = employees.sort("salary")
  sortedBySalary.show()

  // Sort by date and then salary
  val sortedByDateAndSalary = employees.sort("date", "salary")
  sortedByDateAndSalary.show()

  // Sort by salary in descending order
  val sortedBySalaryDesc = employees.sort(desc("salary"))
  sortedBySalaryDesc.show()
}

