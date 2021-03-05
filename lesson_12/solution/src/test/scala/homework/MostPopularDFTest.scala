package homework

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.QueryTest.checkAnswer
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{Row, SQLContext, SQLImplicits, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class SharedTest extends SharedSparkSession {
  import testImplicits._

  test("join - processTaxiData") {

  }
}
