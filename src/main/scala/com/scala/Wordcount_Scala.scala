package com.scala



object Wordcount_Scala {
  def main(args: Array[String]): Unit = {
    val list = List("Jayachandran is working on BigData Technologies","Hello Jayachandran","BigData")
    val words = list.flatMap(line => line.split(" "))
    val keyData = words.map(word => (word,1))
    val groupedData = keyData.groupBy(_._1)
    val result = groupedData.mapValues(list=>  list.map(_._2).sum )
    println(words)
    println(keyData)
    println(groupedData)
    result.foreach(print)

  }
}
