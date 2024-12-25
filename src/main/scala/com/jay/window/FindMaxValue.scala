package com.jay.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.max

object FindMaxValue {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder.appName("FindMaxValue")
      .master("local[*]").getOrCreate()

    val df = spark.read.format("csv").option("header", "true")
      .option("inferSchema", "true").load("src/main/resources/window/emp_data.txt")

    df.printSchema()
    df.show()

    df.createOrReplaceTempView("Employee")
    // Nth highest salaryxz
    spark.sql("select * from ( select  e.*, DENSE_RANK() over (ORDER BY sal Desc ) as row_no from Employee e )  where row_no = 1").show()
    //spark.sql("select * from ( select  sal, ROW_NUMBER() over (ORDER BY sal Desc ) as row_no from Employee  group by sal ) res where res.row_no = 1").show()

    val byDepName = Window.partitionBy("deptno")
    df.withColumn("mx", (max("sal") over byDepName))
    df.show()
  }
}
