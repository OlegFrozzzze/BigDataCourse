package com.mycompany.project.model

object Main2 extends App {
  def Array(array: Array[Int]): Unit = {
    try {
      if (array.isEmpty) {
        throw new IllegalArgumentException("Ошибка: передан пустой массив!")
      }
      val newArray = array.map(_ + 10)
      println(newArray.mkString("Array(", ", ", ")"))
    } catch {
      case e: IllegalArgumentException =>
        println(e.getMessage)
        sys.exit(1)
    }
  }
  }