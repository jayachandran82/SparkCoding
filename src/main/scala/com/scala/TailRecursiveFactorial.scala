package com.scala

object TailRecursiveFactorial {
  def main(args: Array[String]): Unit = {
    println("Factorial Value : " + factorial(5))
  }

  def factorial(n: BigInt): BigInt = {

    if (n <= 1)
      1
    else
      n * factorial(n - 1)

  }
}
