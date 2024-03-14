import org.apache.spark.sql.SparkSession

object IncrLoad {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("IncrementalLoadTest")
      .getOrCreate()

    // Define PostgreSQL connection properties
    val postgresUrl = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
    val postgresProperties = new java.util.Properties()
    postgresProperties.put("user", "consultants")
    postgresProperties.put("password", "WelcomeItc@2022")
    postgresProperties.put("driver", "org.postgresql.Driver")

    try {
      // Read existing data from Hive table
      val existingData = spark.read.format("parquet").table("usukprjdb.people") // Read the existing table directly

      // Read new data from PostgreSQL with column name mapping
      val whereCondition = """"people_id" > 15"""
      val query = s"(SELECT people_id AS people_id, name AS full_name, age AS current_age, occupation AS occupation FROM people WHERE $whereCondition) AS data"
      val newData = spark.read.jdbc(postgresUrl, query, postgresProperties)

      // Rename columns to align with Hive schema
      val renamedNewData = newData.withColumnRenamed("people_id", "people_id").withColumnRenamed("full_name", "name").withColumnRenamed("current_age", "age").withColumnRenamed("occupation", "occupation")

      // Identify new rows by performing a left anti join
      val incrementalData = renamedNewData.join(existingData, renamedNewData("people_id") === existingData("people_id"), "left_anti")

      println("New_Data", renamedNewData.count()) 
      println("Existing_Data", existingData.count())
      if (renamedNewData.count() == incrementalData.count() + existingData.count()){
        println("Count Matches")
      }

      if (incrementalData.isEmpty) {
        println("No new data to load. Incremental load test passed.")
      } else {
        // Append new data to Hive table
        incrementalData.write.mode("append").format("parquet").saveAsTable("usukprjdb.people")
        println("Incremental load successful.")
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
