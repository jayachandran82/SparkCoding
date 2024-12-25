package com.jay.explode

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, row_number}

object EmpOrderTicketMultiJSON {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("EmpOrdTicketMultiJSON ")
      .master("local[2]").getOrCreate()

    val df = spark.read.option("multiline", "true").json("./src/main/resources/json/EmpOrderTicket.json")
    // Employee Table
    val employeeDF = df.selectExpr("explode(employee) as employee")
      .select(
        col("employee.id"),
        col("employee.name"),
        col("employee.date"),
        col("employee.type")
      )
    employeeDF.printSchema()
    // Order Table
    val orderDF = df.selectExpr("explode(order) as order")
      .select(
        col("order.order_id"),
        col("order.employee_id"),
        col("order.name"),
        col("order.order_date"),
        col("order.order_amount"),
        col("order.type")
      )

    // Tickets Table
    val ticketsDF = df.selectExpr("explode(tickets) as tickets")
      .select(
        col("tickets.ticket_id"),
        col("tickets.order_id"),
        col("tickets.name"),
        col("tickets.ticket_status"),
        col("tickets.type")
      )

    // Join tickets with orders
    val ticketsOrdersDF = ticketsDF
      .join(orderDF, ticketsDF("order_id") === orderDF("order_id"))
      .select(ticketsDF("ticket_id"), ticketsDF("order_id"), ticketsDF("ticket_status"), orderDF("employee_id"))

    // Join the result with employees to get the department
    val ticketsOrdersEmployeesDF = ticketsOrdersDF
      .join(employeeDF, ticketsOrdersDF("employee_id") === employeeDF("id"))
      .select(col("ticket_id"), col("order_id"), col("ticket_status"), col("employee_id"), col("type"), col("date"))

    // Group by date and department to count the number of tickets
    val ticketsPerDepartmentPerDayDF = ticketsOrdersEmployeesDF
      .groupBy(col("date"), col("type"))
      .agg(count("ticket_id").alias("ticket_count"))

    // Find the department with the highest number of tickets for each day
    val windowSpec = Window.partitionBy("date").orderBy(col("ticket_count").desc)

    val rankedTicketsDF = ticketsPerDepartmentPerDayDF
      .withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") === 1)
      .select("date", "type", "ticket_count")

    // Show the result
    rankedTicketsDF.show()
  }

}
