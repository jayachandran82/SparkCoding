package com.jay.regexp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{dense_rank, lit}

case class Salary(empid: String, salary: Int)

object LitDataFrame {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Lit Example").master("local").getOrCreate()

    import spark.sqlContext.implicits._
    val data = Seq(Salary("111", 50000), Salary("765", 40000), Salary("222", 60000), Salary("333", 40000), Salary("444", 55000), Salary("555", 40000))
    val df = data.toDS()
    df.show()
    val df2 = df.select($"EmpId", $"Salary", lit(1).as("Lit_value"))
    df2.show()

    val partitionby = Window.partitionBy("Salary").orderBy($"EmpId")
    val windf = df2.withColumn("row_number", dense_rank() over(partitionby))
    windf.show()

    /*val df3 = df2.filter($"EmpId" !== "111")
    df3.show()*/
  }
}
