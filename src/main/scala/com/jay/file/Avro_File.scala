package com.jay.file

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Avro_File {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val data = Seq(("James ","","Smith",2018,1,"M",3000),
      ("Michael ","Rose","",2010,3,"M",4000),
      ("Robert ","","Williams",2010,3,"M",4000),
      ("Maria ","Anne","Jones",2005,5,"F",4000),
      ("Jen","Mary","Brown",2010,7,"",-1)
    )
    val spark = SparkSession.builder().appName("Avro File")
      .master("local[2]")
      .getOrCreate()

    val columns = Seq("firstname", "middlename", "lastname", "dob_year",
      "dob_month", "gender", "salary")
    //import spark.sqlContext.implicits._
    import spark.implicits._
    val avroDF = data.toDF(columns:_*)
    avroDF.show(false)

    avroDF.write.format("avro").save("C:\\_BigData\\Scala\\04_Source\\SparkGradle\\src\\main\\resources\\users.avro")
    //avroDF.write.format("avro").save(".\\src\\main\\resources\\users.avro")
    /*avroDF.write.partitionBy("dob_year","dob_month")
      .format("avro").save("./src/main/resources/users.avro")*/

   /* val df = spark.read.format("avro").load("./src/main/resources/users.avro")
    df.printSchema()
    df.show()*/
  }
}
