package com.jay.interview

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object JSON2Dataset {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("JSON_Sample").master("local[1]") getOrCreate()

    val jsonStr = """[{"a":1,"b":2,"c":3}, {"a":11,"b":12,"c":13},{"a":1,"b":2,"c":3}]"""
    import spark.implicits._
    val df= spark.read.json(spark.createDataset(jsonStr :: Nil))
    df.show()
    val exprs = df.columns.map((_ -> "collect_list")).toMap
    println(exprs)
    df.agg(exprs).show()


  }
}
