package com.jay.common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object GroupByExample {

  def main(args : Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("GroupExample").master("local").getOrCreate()

    val sedDf=Seq("Line number one has six words","Line number two has has two words")

    import spark.implicits._
    val rdd = sedDf.flatMap(r =>r.split(" "))
    val data = rdd.toDF("Name")
    data.printSchema()
    data.show()
    val groupDf = data.groupBy("Name").count()
    groupDf.show()

  }



}
