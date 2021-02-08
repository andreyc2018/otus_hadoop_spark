package org.example

import com.dimafeng.testcontainers.scalatest.TestContainerForAll
import com.dimafeng.testcontainers.PostgreSQLContainer
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.DriverManager
import java.util.Properties

class PostgresqlSpec extends AnyFlatSpec with TestContainerForAll {

  override val containerDef = PostgreSQLContainer.Def()

  val testTableName = "users"

  def testRead(spark: SparkSession, partitionSize: Int, recordsUnderTest: Int,
               postgresServer: Containers): Boolean = {

    var extraPartition = 0
    var expectedPartitions = 0
    if (partitionSize > 0) {
      extraPartition = if ((recordsUnderTest % partitionSize) > 0) 1 else 0
      expectedPartitions = recordsUnderTest / partitionSize + extraPartition
    }
//    val expectedPartitions =
//      if ((recordsUnderTest%partitionSize) > 0) recordsUnderTest/partitionSize + 1
//      else recordsUnderTest/partitionSize

    val df = spark
      .read
      .format("org.example.datasource.postgres")
      .option("url", postgresServer.jdbcUrl)
      .option("user", postgresServer.username)
      .option("password", postgresServer.password)
      .option("tableName", testTableName)
      .option("partitionSize", partitionSize)
      .load()

    df.show(1)

    println(s"testing size: $partitionSize, expected: $expectedPartitions, got: ${df.rdd.getNumPartitions}")
    df.rdd.getNumPartitions == expectedPartitions
  }

  "PostgreSQL data source" should "read table" in withContainers { postgresServer =>
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PostgresReaderJob")
      .getOrCreate()

    val recordsUnderTest = 50

    (0 to 60 by 7).map(
      s => assert(testRead(spark, s, recordsUnderTest, postgresServer))
    )

    spark.stop()

  }

  "PostgreSQL data source" should "write table" in withContainers { postgresServer =>
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("PostgresWriterJob")
      .getOrCreate()

    import spark.implicits._

    val df = (60 to 70).map(_.toLong).toDF("user_id")

    df
      .write
      .format("org.example.datasource.postgres")
      .option("url", postgresServer.jdbcUrl)
      .option("user", postgresServer.username)
      .option("password", postgresServer.password)
      .option("tableName", testTableName)
      .option("partitionSize", 1)
      .mode(SaveMode.Append)
      .save()

    spark.stop()
  }

  override def afterContainersStart(container: Containers): Unit = {
    super.afterContainersStart(container)

    container match {
      case c: PostgreSQLContainer => {
        val conn = connection(c)
        val stmt1 = conn.createStatement
        stmt1.execute(Queries.createTableQuery)
        val stmt2 = conn.createStatement
        stmt2.execute(Queries.insertDataQuery)
        conn.close()
      }
    }
  }

  def connection(c: PostgreSQLContainer) = {
    Class.forName(c.driverClassName)
    val properties = new Properties()
    properties.put("user", c.username)
    properties.put("password", c.password)
    DriverManager.getConnection(c.jdbcUrl, properties)
  }

  object Queries {
    lazy val createTableQuery = s"CREATE TABLE $testTableName (user_id BIGINT PRIMARY KEY);"

    lazy val testValues: String = (1 to 50).map(i => s"($i)").mkString(", ")

    lazy val insertDataQuery = s"INSERT INTO $testTableName VALUES $testValues;"
  }

}
