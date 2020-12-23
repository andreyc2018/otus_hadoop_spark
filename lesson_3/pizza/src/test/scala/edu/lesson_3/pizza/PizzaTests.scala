package edu.lesson_3.pizza

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter

class PizzaTests extends AnyFunSuite with BeforeAndAfter {
  var pizza: Pizza = _

  before {
    println("Before")
    pizza = new Pizza
  }

  after {
    println("after")
  }

  test("new pizza has zero toppings") {
    println("new pizza has zero toppings")
    assert(pizza.getToppings.isEmpty)
  }

  test("adding one topping") {
    println("adding one topping")
    pizza.addTopping(Topping("green olives"))
    assert(pizza.getToppings.size === 1)
  }

  // mark that you want a test here in the future
  test ("test pizza pricing") (pending)
}
