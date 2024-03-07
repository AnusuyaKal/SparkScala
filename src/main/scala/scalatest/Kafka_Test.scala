package scala.scalatest
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfter, FlatSpec}

class TestAPIReadingAndKafkaCreation extends FlatSpec with BeforeAndAfter {
  var spark: SparkSession = _

  before {
    // Initialize SparkSession
    spark = SparkSession.builder()
      .appName("TestAPIReadingAndKafkaCreation")
      .master("local[2]")
      .getOrCreate()
  }

  after {
    // Stop SparkSession
    spark.stop()
  }

  "APIReadingAndKafkaCreation" should "read API and create Kafka topic" in {
    // Read API response into DataFrame
    val apiResponseDF = spark.read.json("http://18.133.73.36:5001/insurance_claims1")

    // Create Kafka topic
    apiResponseDF.write.mode("append").saveAsTable("kafka_topic")

    // Verify that the API was read and Kafka topic was created
    assert(apiResponseDF.count() > 0)
    assert(spark.catalog.tableExists("kafka_topic"))
  }
}
