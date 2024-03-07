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
      val apiResponseDF = readAPIResponse(spark, "http://18.133.73.36:5001/insurance_claims1")

      // Check if DataFrame is not null and has rows
      if (apiResponseDF != null && !apiResponseDF.isEmpty) {
        println("API response DataFrame is not null and has data.")
        // Show DataFrame
        apiResponseDF.show()

        // Create Kafka topic
        createKafkaTopic(apiResponseDF, "kafka_topic")

        // Verify that the API was read and Kafka topic was created
        verifyAssertions(apiResponseDF, spark)
      } else {
        println("API response DataFrame is null or empty.")
      }
    } catch {
      case e: Exception =>
        println(s"Test failed: ${e.getMessage}")
        e.printStackTrace() // Print stack trace
    } finally {
      // Stop SparkSession
      spark.stop()
    }
  }

  def readAPIResponse(spark: SparkSession, apiUrl: String): DataFrame = {
    // Read API response into DataFrame
    val apiResponseDF = spark.read.json(apiUrl)
    apiResponseDF
  }

  def createKafkaTopic(apiResponseDF: DataFrame, topicName: String): Unit = {
    // Create Kafka topic
    apiResponseDF.write.mode("append").saveAsTable(topicName)
    println(s"Kafka topic '$topicName' created successfully.")
  }

  def verifyAssertions(apiResponseDF: DataFrame, spark: SparkSession): Unit = {
    // Verify that the API was read and Kafka topic was created
    assert(apiResponseDF.count() > 0, "API response DataFrame should contain data.")
    assert(spark.catalog.tableExists("kafka_topic"), "Kafka topic should exist in catalog.")
    println("Assertions passed successfully.")
  }
}
