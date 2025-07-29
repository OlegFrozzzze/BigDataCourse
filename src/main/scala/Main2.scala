package com.mycompany.project.model

object Main2 extends App {
  private val originalArray = Array(1, 2, 3, 4, 5)
  private val newArray = originalArray.map(_ + 10)
  println(newArray.mkString("Array(", ", ", ")"))
}

