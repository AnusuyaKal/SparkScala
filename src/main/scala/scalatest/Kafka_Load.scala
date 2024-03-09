package scalatest

import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Admin}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, HColumnDescriptor, HTableDescriptor}
import java.util.concurrent.TimeUnit
import scala.util.Random
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, Encoder}
import org.apache.spark.sql.Encoders
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Admin}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, HColumnDescriptor, HTableDescriptor}


object Kafka_Load extends App {
  
  // Define the Kafka topic and HBase table name
  val topic = "insurance_claims_5-3-12-98"
  val tableName = "insurance_claims_5-3-12-98"

  // Initialize Spark session
  val spark = SparkSession.builder.appName("API to Kafka and HBase").getOrCreate()

  // Import Spark SQL implicits
  import spark.implicits._

  // Function to fetch data from API
  def fetchDataFromAPI(url: String): String = {
    Source.fromURL(url).mkString
  }

  // Function to generate random data
  def generateRandomData(numRecords: Int): Seq[String] = {
    val random = new Random()
    (1 to numRecords).map(_ => random.nextInt(1000).toString)
  }

  // Function to publish data to Kafka
  def publishToKafka(data: Seq[String], kafkaServers: String): Unit = {
    data.toDF("value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("topic", topic)
      .save()
  }

  // Function to consume data from Kafka and load it into HBase
  def consumeAndLoadToHBase(kafkaServers: String): Unit = {
    val df = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    // Convert Kafka messages to string for processing
    val messages = df.selectExpr("CAST(value AS STRING) as message").as[String]


    // Configure HBase connection
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "ip-172-31-3-80.eu-west-2.compute.internal,ip-172-31-5-217.eu-west-2.compute.internal,ip-172-31-9-237.eu-west-2.compute.internal")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("zookeeper.znode.parent", "/hbase")

    // Establish HBase connection
    val connection = ConnectionFactory.createConnection(hbaseConf)

    // Get the HBase table
    val table = connection.getTable(TableName.valueOf(tableName))

    // Insert each Kafka message into HBase
    messages.collect().foreach { message =>
      val put = new Put(Bytes.toBytes(message))
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("column"), Bytes.toBytes(message))
      table.put(put)
    }

    // Close HBase table and connection to clean up resources
    table.close()
    connection.close()
  }

  // Kafka servers configuration
  val kafkaServers = "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092"

  // Schedule tasks to run periodically
  while (true) {
    // Fetch data from API and publish to Kafka every 5 seconds
    val result = fetchDataFromAPI("http://18.133.73.36:5001/insurance_claims1")
    val jsonData = spark.read.json(Seq(result).toDS)
    val urlRowCount = jsonData.count()
    jsonData.selectExpr("to_json(struct(*)) AS value").write.format("kafka").option("kafka.bootstrap.servers", kafkaServers).option("topic", topic).save()
    println(s"Published data to Kafka. Number of rows in DataFrame from URL: $urlRowCount")

    // Load data from Kafka to HBase every 10 seconds
    consumeAndLoadToHBase(kafkaServers)
    println("Loaded data from Kafka to HBase.")

    // Wait for 5 seconds before fetching data from API again
    TimeUnit.SECONDS.sleep(5)
  }

  // Stop Spark session
  spark.stop()
}
