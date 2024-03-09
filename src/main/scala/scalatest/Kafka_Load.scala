import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Admin}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, HColumnDescriptor, HTableDescriptor}
import java.util.concurrent.TimeUnit
import scala.util.Random

object Kafka_Load extends App {

  //####################################################
  // Parameters for the script
  val url   = "http://18.133.73.36:5001/insurance_claims1" // API URL to fetch the JSON data
  val topic = "insurance_claims_5-3-12-98" // Kafka topic name & table name in HBase (or Hive)
  //####################################################

  // Initialize Spark session for DataFrame and Dataset APIs
  val spark = SparkSession.builder.appName("API to Kafka _ 2").getOrCreate()

  // Correctly import Spark SQL implicits
  import spark.implicits._

  // Fetch data from URL and convert it to a string
  val result = Source.fromURL(url).mkString

  // Read the JSON data into a DataFrame
  val jsonData = spark.read.json(Seq(result).toDS)
  jsonData.show() // Display the DataFrame contents

  // Kafka servers configuration
  val kafkaServers = "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092"

  // Function to publish data to Kafka
  def publishToKafka(data: String, topic: String, kafkaServers: String, spark: SparkSession): Unit = {
    val jsonData = spark.read.json(Seq(data).toDS())
    jsonData.selectExpr("to_json(struct(*)) AS value").write.format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("topic", topic)
      .save()
  }

  // Function to consume data from Kafka and load it into HBase
  def consumeAndLoadToHBase(topic: String, kafkaServers: String, spark: SparkSession): Unit = {
    val df = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()

    val messages = df.selectExpr("CAST(value AS STRING) as message")

    // Configure HBase connection
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "zookeeper-server1,zookeeper-server2,zookeeper-server3")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("zookeeper.znode.parent", "/hbase")

    // Establish HBase connection
    val connection = ConnectionFactory.createConnection(hbaseConf)

    // Prepare HBase table for storing the messages
    val tableName = TableName.valueOf(topic)
    val columnFamilyName = "cf"
    val admin = connection.getAdmin

    // Check if table exists, if not, create it
    if (!admin.tableExists(tableName)) {
      val tableDescriptor = new HTableDescriptor(tableName)
      val columnDescriptor = new HColumnDescriptor(columnFamilyName)
      tableDescriptor.addFamily(columnDescriptor)
      admin.createTable(tableDescriptor)
      println(s"Table $tableName created.")
    }

    // Get the HBase table
    val table = connection.getTable(tableName)

    // Function to generate unique row keys for messages
    def generateUniqueRowKey(): String = {
      val currentTime = System.currentTimeMillis()
      val random = new Random()
      s"$currentTime-${random.nextInt(10000)}"
    }

    // Insert each Kafka message into HBase
    messages.collect().take(1000).foreach { message =>
      val rowKey = generateUniqueRowKey()
      val put = new Put(Bytes.toBytes(rowKey))
      put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("data"), Bytes.toBytes(message.getString(0)))
      table.put(put)
    }

    // Print summary of operations
    println("Finished loading data to HBase.")

    // Close HBase table and connection to clean up resources
    table.close()
    connection.close()
  }

  // Kafka servers configuration
  // val kafkaServers = "kafka-server1:9092,kafka-server2:9092,kafka-server3:9092"
  // val topic = "insurance_claims_topic"

  // Produce data every 5 seconds
  while (true) {
    val data = fetchDataFromAPI("http://18.133.73.36:5001/insurance_claims1")
    publishToKafka(data, topic, kafkaServers, spark)
    println(s"Published data to Kafka at ${java.time.LocalDateTime.now()}")
    TimeUnit.SECONDS.sleep(5)
  }

  // Consume data and load into HBase every 10 seconds
  while (true) {
    consumeAndLoadToHBase(topic, kafkaServers, spark)
    println(s"Loaded data from Kafka to HBase at ${java.time.LocalDateTime.now()}")
    TimeUnit.SECONDS.sleep(10)
  }

  // Stop Spark session
  spark.stop()
}
