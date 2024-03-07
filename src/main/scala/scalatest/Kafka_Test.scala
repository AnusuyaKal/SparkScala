package scalatest
import org.apache.spark.sql.{DataFrame, SparkSession}

object Kafka_Test {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("TestAPIReadingAndKafkaCreation")
      .master("local[2]")
      .getOrCreate()

    try {
      // Read API response into DataFrame
      val apiResponseDF = spark.read.json("http://18.133.73.36:5001/insurance_claims1")

      // Check if DataFrame is not null and has rows
      if (apiResponseDF != null && !apiResponseDF.isEmpty) {
        println("show", apiResponseDF)
        // Show DataFrame
        apiResponseDF.show()

        // Create Kafka topic
        apiResponseDF.write.mode("append").saveAsTable("kafka_topic")

        // Verify that the API was read and Kafka topic was created
        assert(apiResponseDF.count() > 0)
        assert(spark.catalog.tableExists("kafka_topic"))
      } else {
        println("API response DataFrame is null or empty.")
      }
    } catch {
      case e: Exception =>
        println(s"Test failed: ${e.getMessage}")
        e.printStackTrace() // Print stack trace
      case _: Throwable =>
        println("An unexpected error occurred.")
    } finally {
      // Stop SparkSession
      spark.stop()
    }
  }
}
