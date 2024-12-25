package com.jay.common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DistinctValueSplitFunction {
  case class TvDataSchema(tvid:String,  timestamp:String, event:String)
  def mapper(line: String): TvDataSchema = {
    val fields = line.split("\\|")
    val tvdata: TvDataSchema = TvDataSchema(fields(0), fields(1),fields(2))
    return tvdata
  }
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("DistinctValue ")
      .master("local[2]").getOrCreate()

    val txtfile = "./src/main/resources/common/tvevent.txt"
    /* //val schemaString = "tvid  timestamp event"
      A|10:00AM|EV001
      B|10:10AM|EV002
      A|10:11AM|EV001
      B|10:12AM|EV002
      C|10:20AM|EV001*/
    val lines = spark.read.textFile(txtfile)
    lines.show()
    import spark.implicits._
    val df = lines.map( row=> mapper(row))
    df.printSchema()
    df.show()
  }
}
