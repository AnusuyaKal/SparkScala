import org.apache.spark.sql.SparkSession

object Kafka_Test_Test {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession for testing
    val spark = SparkSession.builder()
      .appName("Kafka_Test_Test")
      .master("local[2]")
      .getOrCreate()

    // Test case for Kafka_Test functionality
    testKafka_TestFunctionality(spark)

    // Stop SparkSession after test
    spark.stop()
  }

  // Test Kafka_Test functionality
  def testKafka_TestFunctionality(spark: SparkSession): Unit = {
    val url = "http://18.133.73.36:5001/insurance_claims1"
    val topic = "insurance_claims_5-3-12-98"

    // Execute Kafka_Test code
    Kafka_Test.main(Array())

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
  }
}
