package com.jay.common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SeqToSub {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("SeqToJSON")
      .master("local[*]").getOrCreate()

    val seqData = spark.createDataFrame(Seq((1, 2), (1, 0))).toDF("Col1", "col2")
    seqData.printSchema()
    val sqlDF = seqData.withColumn("Difference", col("Col1") - col("col2"))
    sqlDF.createOrReplaceTempView("Calculate")
    spark.sql("select * from Calculate").show()

    import spark.implicits._
    val seqDatSet = spark.createDataset(Seq((1, 2), (1, 0))).toDF("A", "B")
    seqDatSet.printSchema()
    val sqlDS = seqDatSet.withColumn("Sub", col("A") - col("B"))
    sqlDS.select("A", "Sub").show()
  }
}
