package edu.lesson_3.pizza

import scala.collection.mutable.ArrayBuffer

class Pizza {
  private val toppings = new ArrayBuffer[Topping]

  def addTopping (t: Topping): Unit = { toppings += t}
  def removeTopping (t: Topping): Unit = { toppings -= t}
  def getToppings: Seq[Topping] = toppings.toList

  def boom: Unit = { throw new Exception("Boom!") }
}
