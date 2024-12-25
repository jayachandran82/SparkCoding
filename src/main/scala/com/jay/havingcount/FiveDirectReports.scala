package com.jay.havingcount

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.count

/**
 * SQL Having equal ==>  .filter($"column" >= 5)
 */
object FiveDirectReports {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("FiveDirectReports").getOrCreate()

    import spark.implicits._

    // Sample data
    val data = Seq(
      (1, "Alice", 10),
      (2, "Bob", 10),
      (3, "Cathy", 20),
      (4, "David", 10),
      (5, "Eve", 20),
      (6, "Frank", 30),
      (7, "Grace", 10),
      (8, "Hannah", 20),
      (9, "Ian", 10),
      (10, "Jack", 10)
    )

    // Create DataFrame
    val df = data.toDF("id", "name", "managerId")

    // Define window specification to partition by managerId
    val windowSpec = Window.partitionBy("managerId")

    // Count the number of employees per manager
    val managerCountsDF = df.withColumn("managerslist", count("managerId").over(windowSpec))
    managerCountsDF.show(false)

    // Filter managers who have at least 5 employees
    val filteredManagersDF = managerCountsDF.filter($"managerslist" >= 5).select("managerId").distinct()

    // Join the filtered managers with the original DataFrame to get the names of employees
    val resultDF = df.join(filteredManagersDF, "managerId").select("name")

    // Show the result
    resultDF.show()
  }

}
