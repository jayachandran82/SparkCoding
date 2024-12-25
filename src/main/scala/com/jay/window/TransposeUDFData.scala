package com.jay.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{array, col, explode, lit, struct}

object TransposeUDFData {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val empFile = "src/main/resources/window/Pharmacy.csv"
    val spark = SparkSession.builder().appName("Join CSV File")
      .master("local[2]").getOrCreate();

    val pharmcyDf = spark.read.format("csv")
      .option("header", "true").option("charset", "UTF8").load(empFile)

    pharmcyDf.printSchema()
    pharmcyDf.show()

    import spark.implicits._
    def transposeUDF(transDF: DataFrame, transBy: Seq[String]): DataFrame = {
      val (cols, types) = transDF.dtypes.filter { case (c, _) => !transBy.contains(c) }.unzip
      require(types.distinct.size == 1)

      val kvs = explode(array(
        cols.map(c => struct(lit(c).alias("metrics"), col(c).alias("column_value"))): _*
      ))

      val byExprs = transBy.map(col(_))

      transDF
        .select(byExprs :+ kvs.alias("_kvs"): _*)
        .select(byExprs ++ Seq($"_kvs.metrics", $"_kvs.column_value"): _*)
    }

    val pdf = transposeUDF(pharmcyDf, Seq("client", "Supplier", "SupID", "facility", "otlt_id", "otlt_cd"
      , "otlt_nm", "otlt_addr", "otlt_city", "otlt_st", "otlt_zip", "Datamonth"))

    pdf.show(100)

    /*val opDF =  pdf.groupBy("client", "Supplier","SupID","facility","otlt_id","otlt_cd","otlt_nm","otlt_addr","otlt_city","otlt_st","otlt_zip","metrics")
      .pivot("Datamonth")
      .agg(sum("column_value")).na.fill(0) //when(sum("column_value").isNull, 0).otherwise(sum("column_value"))
      .sort("otlt_cd","otlt_addr","metrics")

    opDF.show(100)*/

  }
}
