package com.jay.interview

//  Scala avoids the diamond problem by something called "trait linearization"
//  Basically, it looks up the method implementation in the traits you extend from right to left.
trait one {
  val name: String = "Jayachandran"

  def show(): Unit = {
    println("one show function")
  }

  def display()
}

trait two extends one {
  override def display(): Unit = {
    println("two Show function " + name)
  }
}

trait three extends one {
  override def display(): Unit = {
    println("Three display function ")
  }

  override def show(): Unit = {
    println("Three show function")
  }
}

class four extends three with two

object TraitTest {

  def main(args: Array[String]): Unit = {
    val obj = new four
    obj.show()
    obj.display()

  }

}
