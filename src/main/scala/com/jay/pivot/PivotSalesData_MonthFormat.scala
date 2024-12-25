package com.jay.pivot

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SparkSession, functions}

/**
 * Input Sales data pivot to month
 * ----------------------------------
 * product	month	sales
 * A	Jan	100
 * A	Feb	150
 * A	Mar	200
 * B	Jan	80
 * B	Feb	120
 * B	Mar	160
 * C	Jan	200
 * C	Feb	250
 * C	Mar	300
 *
 * Result
 * -------------------------------------------
 * +-------+---+---+---+
 * |product|Jan|Feb|Mar|
 * +-------+---+---+---+
 * |      A|100|150|200|
 * |      B| 80|120|160|
 * |      C|200|250|300|
 * +-------+---+---+---+
 *
 */
object PivotSalesData_MonthFormat {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("PivotSalesDatatoMonthFormat ")
      .master("local[2]").getOrCreate()
    import spark.implicits._

    // Sample data
    val data = Seq(
      ("A", "Jan", 100),
      ("A", "Feb", 150),
      ("A", "Mar", 200),
      ("B", "Jan", 80),
      ("B", "Feb", 120),
      ("B", "Mar", 160),
      ("C", "Jan", 200),
      ("C", "Feb", 250),
      ("C", "Mar", 300)
    )

    // Create DataFrame
    val df = data.toDF("product", "month", "sales")

    // Pivot the DataFrame
    val pivotDF = df.groupBy("product")
      .pivot("month")
      .agg(functions.sum("sales"))

    // Show the result
    pivotDF.show()

  }

}
