package com.jay.common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SeqWithCase {
  case class Person(name: String, age:Int)
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("SeqWithClass").master("local").getOrCreate()
    val seq = Seq(("Jay",37),("Ramesh",40),("Mani", 25), ("Madhu", 27))

    import spark.implicits._
    val df = seq.toDF("name","age")
    df.printSchema()
    df.show()
    val df2 =df.as[Person]
    df2.printSchema()
    df2.select("name").show()

    val data = Seq(("A", 25), ("B", 15))
    val df_filter = data.toDF("name","age")
    df_filter.show()
    val df_Result = df_filter.filter($"age" > 15)
    df_Result.show()

  }

}
