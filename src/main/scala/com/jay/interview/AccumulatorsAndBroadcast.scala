package com.jay.interview

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator
import ch.qos.logback.classic.{Logger => LogbackLogger, Level}
import org.slf4j.LoggerFactory

object AccumulatorsAndBroadcast {
  def main(args: Array[String]): Unit = {
    val rootLogger = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[LogbackLogger]
    rootLogger.setLevel(Level.OFF)
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("FirstLastWindowsFunction").getOrCreate()

    val sc = spark.sparkContext

    // Create an accumulator
    val accum: LongAccumulator = sc.longAccumulator("My Accumulator")

    // Create a broadcast variable
    val broadcastVar = sc.broadcast(Array(1, 2, 3, 4, 5))

    // Sample data
    val data = sc.parallelize(1 to 100)

    // Use the accumulator
    data.foreach(x => accum.add(x.toLong)) // Ensure x is Long

    // Use the broadcast variable
    val broadcastSum = broadcastVar.value.map(_.toLong).sum // Ensure elements are Long
    val result = data.map(x => x * broadcastSum)

    // Print the results
    println(s"Accumulator value: ${accum.value}")
    result.collect().foreach(println)

    // Stop the Spark session
    spark.stop()

  }

}
