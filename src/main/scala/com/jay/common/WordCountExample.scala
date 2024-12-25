package com.jay.common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCountExample {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Word Count Example").master("local").getOrCreate()
    //val df = spark.read.text("./src/main/resources/common/wordcount.txt").toDF("line")
    val rdd = spark.sparkContext.textFile("./src/main/resources/common/wordcount.txt")
    val df = rdd.flatMap(r =>r.split(" ")).map(row =>(row,1))
    val reducedata = df.reduceByKey(_ + _)
    val counts = reducedata.collect
    counts.foreach(print)
  }
}
