import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col

object Kafka_Test {
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Kafka_Test")
      .master("local[2]")
      .getOrCreate()

    try {
      // Parameters
      val url = "http://18.133.73.36:5001/insurance_claims1"
      val topic = "insurance_claims_5-3-12-98"

      // Read JSON data from URL into DataFrame
      val jsonData = spark.read.json(url)

      // Show DataFrame contents
      jsonData.show()

      // Kafka server configuration
      val kafkaServers = "localhost:9092"

      // Publish DataFrame as JSON string to Kafka topic
      jsonData.select(to_json(struct("*")).alias("value"))
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaServers)
        .option("topic", topic)
        .save()

      // Read from Kafka topic to verify data pipeline
      val df = spark.read.format("kafka")
        .option("kafka.bootstrap.servers", kafkaServers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()

      // Convert the value column to String type
      val messages = df.withColumn("message", col("value").cast("string")).select("message")

      // Display Kafka messages
      messages.show()

      // Count the number of rows in the jsonData DataFrame
      val urlRowCount = jsonData.count()

      // Count the number of rows in the Kafka DataFrame
      val kafkaRowCount = messages.count()

      // Compare the counts
      if (urlRowCount == kafkaRowCount) {
        println("Counts match: Data was successfully published to Kafka topic.")
      } else {
        println(s"Counts don't match: URL data count = $urlRowCount, Kafka data count = $kafkaRowCount")
      }

      // Stop SparkSession
      spark.stop()
    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
