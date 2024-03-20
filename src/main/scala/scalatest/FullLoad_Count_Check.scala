// package scalatest

// import org.apache.spark.sql.SparkSession
// import org.apache.spark.sql.functions._

// object FullLoad_Count_Check {
//   def main(args: Array[String]): Unit = {
//     // Create SparkSession for testing
//     val spark: SparkSession = SparkSession.builder()
//       .appName("FullLoadPostgresToHiveTest")
//       .master("local[*]")  // Use local mode for testing
//       .getOrCreate()

//     // Define PostgreSQL connection properties for testing
//     val postgresUrl = "jdbc:postgresql://ec2-3-9-191-104.eu-west-2.compute.amazonaws.com:5432/testdb"
//     val postgresProperties = new java.util.Properties()
//     postgresProperties.put("user", "consultants")
//     postgresProperties.put("password", "WelcomeItc@2022")
//     postgresProperties.put("driver", "org.postgresql.Driver")
    
//     try {
//       // Read test data from PostgreSQL into a DataFrame
//       var dfPostgres = spark.read.jdbc(postgresUrl, "people", postgresProperties)

//       // Get the existing column names
//       val existingColumns = dfPostgres.columns

//       // Generate new column names by appending a suffix
//       val newColumns = existingColumns.map(_ + "_new")

//       // Create a map of old column names to new column names
//       val columnMap = existingColumns.zip(newColumns).toMap

//       // Apply column renames using foldLeft
//       dfPostgres = columnMap.foldLeft(dfPostgres) { case (df, (oldName, newName)) =>
//         df.withColumnRenamed(oldName, newName)
//       }

//       // Get the counts of records in PostgreSQL and Hive
//       val postgresCount = dfPostgres.count()
//       val hiveDataCount = spark.read.table("usukprjdb.people").count()
      
//       // Verify if the counts match
//       if (postgresCount == hiveDataCount) {
//         println("PostgresCount", postgresCount)
//         println("hiveDataCount", hiveDataCount)
//         println("Test passed: Full load from PostgreSQL to Hive successful and Count Matches")
//       } else {
//         println(s"Test failed: Data count mismatch. PostgreSQL count: $postgresCount, Hive count: $hiveDataCount")
//       }
//     } catch {
//       case e: Exception =>
//         println(s"Test failed: ${e.getMessage}")
//     } finally {
//       // Stop SparkSession after testing
//       spark.stop()
//     }
//   }
// }
