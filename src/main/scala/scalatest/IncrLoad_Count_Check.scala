package scalatest

import org.apache.spark.sql.{DataFrame, SparkSession}

object IncrLoad_Count_Check {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder
      .appName("TestDataLoading")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    try {
      // Read data from PostgreSQL
      val postgresUrl = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
      val postgresProperties = new java.util.Properties()
      postgresProperties.put("user", "consultants")
      postgresProperties.put("password", "WelcomeItc@2022")
      postgresProperties.put("driver", "org.postgresql.Driver")
      
      val whereCondition = """"people_id" > 14"""
      val query = s"(SELECT * FROM public.people WHERE $whereCondition) AS data"
      println("Generated SQL query:", query)

      // Read new data from PostgreSQL with the WHERE condition
      val postData = spark.read.jdbc(postgresUrl, query, postgresProperties)
      postData.show()
      val postgresCount = postData.count()

      // Read count of rows from Hive table after applying the WHERE condition
      val hiveWhereCondition = "people_id > 14"
      val hiveCountDF = spark.sql(s"SELECT COUNT(*) AS count FROM usukprjdb.people WHERE $hiveWhereCondition")
      val hiveCount = hiveCountDF.collect()(0)(0).toString.toLong

      // Print updated count
      println("Updated_count:", hiveCount)

      if (hiveCount == postgresCount) {
        println("Number of rows loaded to Hive matches the expected count")
        println("Postgres_Count: " + postgresCount)
        println("Hive_count: " + hiveCount)
      } else {
        println("Number of rows loaded to Hive matches the expected count")
        println("Postgres_Count: " + postgresCount)
        println("Hive_count: " + hiveCount)
        assert(hiveCount == postgresCount, "Number of rows loaded to Hive does not match expected count")
      }
    } catch {
      case e: Exception =>
        println(s"Test failed: ${e.getMessage}")
    } finally {
      // Stop SparkSession after testing
      spark.stop()
    }
  }
}
