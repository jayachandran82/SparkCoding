package com.jay.isin

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object IsInCondition {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("IsInCondition ")
      .master("local[2]").getOrCreate()

    // Create a sample DataFrame
    import spark.implicits._

    val data = Seq(
      ("Alice", 29),
      ("Bob", 35),
      ("Cathy", 25),
      ("David", 45)
    ).toDF("name", "age")

    // Print the original DataFrame
    println("Original DataFrame:")
    data.show()

    // List of ages to filter
    val ageList = Seq(29, 35)

    // Apply the isin condition to filter the DataFrame
    val filteredDF = data.filter($"age".isin(ageList: _*))

    // Print the filtered DataFrame
    println("Filtered DataFrame (age in (29, 35)):")
    filteredDF.show()

    // Stop the SparkSession
    spark.stop()
  }

}
