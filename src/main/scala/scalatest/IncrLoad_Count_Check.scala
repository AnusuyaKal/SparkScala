package scalatest;
import org.apache.spark.sql.{DataFrame, SparkSession}

object IncrLoad_Count_Check {
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
      // Read new data schema from PostgreSQL to get column names
      val newDataSchema = spark.read.jdbc(postgresUrl, "people", postgresProperties).schema

      // Define the column name transformation logic
      def transformColumn(name: String): String = {
        // Add your transformation logic here, for example:
        // Convert column names to upper case
        name.toUpperCase
      }

      // Apply column name transformation to the new data DataFrame
      val transformedData = newDataSchema.fields.foldLeft(spark.read.jdbc(postgresUrl, "people", postgresProperties)) {
        (df: DataFrame, field) => df.withColumnRenamed(field.name, transformColumn(field.name))
      }

      val whereCondition = """"people_id" > 11"""  // Define your WHERE condition here

      // Read count of rows from Hive table after applying the WHERE condition
      val hiveQuery = s"SELECT COUNT(*) AS count FROM usukprjdb.people WHERE $whereCondition"
      val hiveCountDF = spark.sql(hiveQuery)
      val hiveCount = hiveCountDF.collect()(0)(0).toString.toLong

      println("Hive Count after incremental load:", hiveCount)

      // Read count of rows from PostgreSQL table after applying the same WHERE condition
      val postgresCountDF = transformedData.where(whereCondition)
      val postgresCount = postgresCountDF.count()

      println("PostgreSQL Count after incremental load:", postgresCount)
    } catch {
      case e: Exception =>
        println(s"Test failed: ${e.getMessage}")
    } finally {
      // Stop SparkSession after testing
      spark.stop()
    }
  }
}
