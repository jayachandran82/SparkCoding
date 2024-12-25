package com.jay.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.max

object WindowFuctionExample {
  case class Salary(dep: String, emp: Long, salary: Long)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder().appName("WindowFuctionExample").master("local[2]").getOrCreate();

    import spark.implicits._
    val empSalary = Seq(Salary("sales", 1, 500),
      Salary("hr", 2, 1980),
      Salary("sales", 3, 4000),
      Salary("sales", 4, 4000),
      Salary("hr", 5, 3560),
      Salary("tech", 6, 6500),
      Salary("tech", 7, 5280),
      Salary("tech", 8, 7000),
      Salary("tech", 9, 1500),
      Salary("tech", 10, 6200),
      Salary("tech", 11, 6353)).toDS

    empSalary.printSchema()
    empSalary.createOrReplaceTempView("empsalary")

    spark.sql("select a.dep, emp, salary, (max_sal -salary) as sal_diff " +
      "from empsalary a  " +
      "inner join (select dep , " +
      "max(salary) as max_sal " +
      "from empsalary group by dep) b on a.dep=b.dep ").show()

    spark.sql("select dep, " +
      "emp, " +
      "salary, " +
      "max (salary) over (partition by dep)-salary as sales_diff " +
      "from empsalary  ").show()

    val salesDesc = Window.partitionBy(empSalary.col("dep")).orderBy(empSalary.col("salary").desc)
    val salesDiff = max(empSalary.col("salary")).over(salesDesc) - empSalary.col("salary")

    empSalary.select('*, salesDiff as "sal_diff").show()
  }
}
