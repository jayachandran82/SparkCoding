package com.jay.common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lag, when}

object IntervalSeq {
  case class Product(timestamp: Int, session_id: String, event_type: String, product_id: String)

  /* def timeToPurchase(events: Seq[Product]): Double = {
     val event = events.aggregate(max(events(p => p.timestamp.cost(Int))) - min(events(p => p.timestamp.cost(Int))))
     return event
   }*/

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Interval Seq").master("local").getOrCreate()

    val seqVal = Seq(Product(12345, "sessionA", "CLICK", "product1"),
      Product(12346, "sessionA", "CLICK", "product2"),
      Product(12347, "sessionA", "PURCHASE", "product1"),
      Product(12348, "sessionB", "CLICK", "product1")
    )
    import spark.sqlContext.implicits._
    val df = seqVal.toDF()
    df.printSchema()
    df.show()
    val partitionBy = Window.partitionBy("product_id").orderBy("session_id")
    val LAG = df.withColumn("LAG", lag($"timestamp", 1).over(partitionBy))
    LAG.show()
    val lagTest =  LAG.withColumn("LAG", when($"LAG".isNull, 0).otherwise($"LAG"))

    lagTest.show()

    val df_test = lagTest.select($"*", ($"timestamp" - $"LAG") as "LAG_Diff")
    df_test.show()
    val finalDf = df_test.withColumn("LAG_Diff", when($"LAG" === 0, 0).otherwise($"LAG_Diff"))
    finalDf.select($"timestamp", $"session_id", $"event_type", $"product_id", $"LAG_Diff".as("timestamp_diff")).show()

    /*val lagTest = df.withColumn("LAG", timeToPurchase(Seq(Product("timestamp".cast(Int), "session_id", "event_type", "product_id")))
                    .over(partitionBy))
      lagTest.show()*/

  }
}
