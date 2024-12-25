package com.jay.common

object SeqExample {
  def main(args: Array[String]): Unit = {
    val seq = Seq(52, 85, 1, 8, 3, 2, 7, 1, 8)
    print("Seq Element : ")
    seq.foreach(element => print(" " + element))
    println("\n distinct : " + seq.distinct)
    println("mkString : " + seq.mkString)
    val seqStr: Seq[String] = Seq("One", "Two", "Three", "Four")
    println("Index value: " + seqStr(3))
    println(s"Display the Seq String Element : $seqStr")
    println(s"Element at index 0 = ${seqStr(0)}")
    val seqStr2: Seq[String] = seqStr :+ "Five"
    println(s"Added seq Element  :+ $seqStr2")
  }
}
