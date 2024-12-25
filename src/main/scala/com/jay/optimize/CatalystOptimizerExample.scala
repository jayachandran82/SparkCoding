package com.jay.optimize

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object CatalystOptimizerExample {

 /* object MultiplyOptimzationRule extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case Multiply(left, right) if right.isInstanceOf[Literal] &&
        right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>
        println("optimization of one applied")
        left
    }
  }*/

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("CatalystOptimizerExample")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/temp")
      .getOrCreate()

    val filePath = "src/main/resources/optimize/Sales.csv"
    val df = spark.read.option("header", "true").csv(filePath)
    df.show()

    val multDF = df.selectExpr("amountPaid * 1")
    println(multDF.queryExecution.executedPlan.numberedTreeString)
    multDF.show(10)

/*    spark.experimental.extraOptimizations = Seq(MultiplyOptimzationRule)
    val dfOpt = df.selectExpr("amountPaid * 1")

    println(dfOpt.queryExecution.executedPlan.numberedTreeString)*/


  }


}

