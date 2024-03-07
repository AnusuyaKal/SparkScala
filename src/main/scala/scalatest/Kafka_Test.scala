import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import spark.implicits._ // Import Spark implicits here
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

      // Now messages is a DataFrame containing only the "message" column with String type


      // Display Kafka messages
      messages.show()

      // Stop SparkSession
      spark.stop()
    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
    }
  }
}
