package com.jay.optimize

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object AdaptiveQueryExecutionExample {
  def main(args: Array[String]): Unit={
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark= SparkSession.builder().appName("Adaptive_Query_Example").master("local[1]").getOrCreate()

    import spark.implicits._
    val simpleData = Seq(("James","Sales","NY",90000,34,10000),
      ("Michael","Sales","NY",86000,56,20000),
      ("Robert","Sales","CA",81000,30,23000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Raman","Finance","CA",99000,40,24000),
      ("Scott","Finance","NY",83000,36,19000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000))
    val df = simpleData.toDF("employee_name","department","state","salary","age","bonus")
    df.printSchema()
    val df1 = df.groupBy("department").count()
    println(df1.rdd.getNumPartitions)
    df1.show()
    spark.conf.set("spark.sql.adaptive.enabled",true)
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled",true)
    val df2=df.groupBy("department").count()
    println(df2.rdd.getNumPartitions)
  }

}
