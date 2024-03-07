import org.apache.spark.sql.SparkSession

object MyMainClassTest {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession for testing
    val spark = SparkSession.builder()
      .appName("MyMainClassTest")
      .master("local[2]")
      .getOrCreate()

    // Test case for MyMainClass functionality
    testMyMainClassFunctionality(spark)

    // Stop SparkSession after test
    spark.stop()
  }

  // Test MyMainClass functionality
  def testMyMainClassFunctionality(spark: SparkSession): Unit = {
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
  }
}
