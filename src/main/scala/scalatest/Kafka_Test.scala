import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.streaming.Trigger
import java.util.Properties

class Kafka_Test extends AnyFunSuite {

  // Create a SparkSession for testing
  val spark = SparkSession.builder()
    .appName("MyMainClassTest")
    .master("local[2]")
    .getOrCreate()

  // Test case for MyMainClass functionality
  test("MyMainClass functionality test") {
    val url = "http://18.133.73.36:5001/insurance_claims1"
    val topic = "insurance_claims_5-3-12-98"

    // Execute MyMainClass code
    MyMainClass.main(Array())

    // Read the DataFrame from Kafka topic
    val kafkaDF = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092") // Use localhost for testing
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    // Convert Kafka messages to strings for comparison
    val messages = kafkaDF.selectExpr("CAST(value AS STRING)").as[String]

    // Verify if messages contain data
    assert(messages.count() > 0)

    // Stop SparkSession after test
    spark.stop()
  }
}
