package com.jay.window

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{SparkSession, functions}

object WindowsHighestSalary {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Highest Salary").master("local").getOrCreate()

    import spark.implicits._
    val empDF = spark.createDataFrame(Seq(
      (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
      (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
      (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
      (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
      (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
      (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
      (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
      (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
      (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
      (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
      (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)))
      .toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")
    empDF.printSchema()
    empDF.show(false)
    val partitionWindow = Window.partitionBy($"deptno").orderBy($"sal".desc)
    //  val partitionWindowRank = functions.rank().over(partitionWindow)
    //empDF.select($"*", partitionWindowRank as "dense_rank").where($"dense_rank" ===2).show()
    val rankDf = empDF.withColumn("Rank", functions.rank().over(partitionWindow)).where($"Rank" === 2)
    rankDf.show()
  }
}
