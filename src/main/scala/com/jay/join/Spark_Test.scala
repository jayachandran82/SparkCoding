package com.jay.join

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col}

/** ***************************************************************************************************
 * There are two text files in c:\data\
 *
 * c:\data\plants.txt
 * c:\data\weather.txt
 *
 * plants.txt is a pipe delimited list of different types of plant species and their types.
 * weather.txt is a pipe delimited list of different plant types and the climate that they thrive.
 *
 * Display a table with two columns, Species and Weather, displaying only the species that thrives in one type of climate.
 *
 * Example:
 *
 * plants.txt
 * species a|type a
 * species b|type b
 *
 * weather.txt
 * type a|climate a
 * type b|climate a
 * type b|climate b
 *
 * final answer:
 *
 * Species|Weather
 * species a|climate a
 *
 * *** Notes
 * * You may use spark sql or dataset/frame APIs.
 * * You may not use the spark context or the sql context directly.
 * * Bonus points if you provide 2 sets of answers using API and Spark SQL.
 * * Try to avoid using RDD APIs if possible.
 * * If you end up using APIs at any point, please print the type of your variable after each set of transformations to show whether they're RDDs, Dataframes, or Datasets.
 * * Don't worry about compiling and deploying the project anywhere.  We will execute the code in the IDE.
 * ************************************************************************************************** */

object Spark_Test {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder.appName("Spark_Test")
      .master("local[*]").getOrCreate()

    val plantData = spark.read.format("csv").option("delimiter", "|")
      .load("src/main/resources/join/plants.txt").toDF("species", "type")
    plantData.show()

    val weatherData = spark.read.format("csv").option("delimiter", "|")
      .load("src/main/resources/join/weather.txt").toDF("type", "climate")
    weatherData.show()

    val joinData = plantData.join(weatherData, plantData("type") === weatherData("type"), "inner").select(col("species"),weatherData.col("type"),col("climate"))
    joinData.show()

    /*val partitionWindow = Window.partitionBy(col("climate")) .orderBy(col("species").desc)
    val orderDf = joinData.drop("type").withColumn("rownum",  row_number.over(partitionWindow))
    orderDf.show()*/

    val orderDF = joinData.select(col("species"), col("climate")).orderBy(col("climate").desc)
    orderDF.show()
  }

}
