package com.jay.common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SeqSplitText {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Seq Split Text").master("local[*]").getOrCreate()
    // Actual : a|15,b|20,c|45,d|10,a|15,a|15,b|20,c|45,d|10,a|15
    //Expecting : (a,45),(b,40),(c,90),(d,20)
    val inputData = Seq("a|15,b|20,c|45,d|10,a|15,a|15,b|20,c|45,d|10,a|15")

    // Convert Seq to Dataset of Rows
    import spark.implicits._
    val inputDS = spark.createDataset(inputData).toDF("line").as[String]
    val df = inputDS.flatMap(_.split(",")) // Split each line by comma to get key-value pairs
    df.show(false)
    val df2 = df.map(kv => {
      val Array(key, value) = kv.split("\\|")
      (key, value.toInt)
    }).groupByKey(_._1) // Group by key


    val df4 = df2.reduceGroups((a, b) => (a._1, a._2 + b._2)) // Reduce values by key
    df4.show(false)
    val df5 = df4.map { case (key, sum) =>
      s"($key,$sum)"
    }

    // Show the output
    df5.show(false)

    // Stop SparkSession
    spark.stop()


  }
}
