package com.jay.common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object ParallelizeDatsetExamp {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder.appName("Parallelize Datset Examp")
      .master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    val values = List(1, 2, 3, 4, 5)
    val distData = sc.parallelize(values)

    import spark.implicits._
    val numDF = distData.toDF("ListNumber")
    numDF.show()

    val seq = Seq((1, 2), (1, 0))
    val seqData = sc.parallelize(seq).toDF("Col1", "Col2")
    seqData.show()

    val numSeq= sc.parallelize(1 to 10)
    val numSeqDf = numSeq.toDF()
    numSeqDf.show()
  }
}
