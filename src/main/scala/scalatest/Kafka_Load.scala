package scalatest

import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Admin}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, HColumnDescriptor, HTableDescriptor}
import org.apache.spark.sql.SparkSession
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Admin}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, HColumnDescriptor, HTableDescriptor}
import java.util.concurrent.TimeUnit
import scala.util.Random
import org.apache.spark.sql.functions._
import spark.implicits._


object Kafka_Load extends App {

  // Function to generate random data
  def generateRandomData(numRecords: Int): Seq[String] = {
    val random = new Random()
    (1 to numRecords).map(_ => random.nextInt(1000).toString)
  }

  // Function to publish data to Kafka
  def publishToKafka(data: Seq[String], topic: String, kafkaServers: String): Unit = {
    val spark = SparkSession.builder.appName("Producer").getOrCreate()
    import spark.implicits._
    spark.sparkContext.parallelize(data).toDF("value")
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("topic", topic)
      .save()
    spark.stop()
  }

  // Function to consume data from Kafka and load it into HBase
  def consumeAndLoadToHBase(topic: String, kafkaServers: String): Unit = {
    val spark = SparkSession.builder.appName("Consumer").getOrCreate()
    val df = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaServers)
      .option("subscribe", topic)
      .load()

    val messages = df.selectExpr("CAST(value AS STRING) as message").as[String]

    // Configure HBase connection
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "ip-172-31-3-80.eu-west-2.compute.internal,ip-172-31-5-217.eu-west-2.compute.internal,ip-172-31-9-237.eu-west-2.compute.internal")
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
      put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("data"), Bytes.toBytes(message))
      table.put(put)
    }

    // Print summary of operations
    println("Finished loading data to HBase.")

    // Close HBase table and connection to clean up resources
    table.close()
    connection.close()

    spark.stop()
  }

  // Kafka servers configuration
  val kafkaServers = "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092"
  val topic = "kafka_hbase_topic"

  // Produce every 5 seconds
  while (true) {
    val data = generateRandomData(1000)
    publishToKafka(data, topic, kafkaServers)
    println("Published data to Kafka.")
    TimeUnit.SECONDS.sleep(5)
    consumeAndLoadToHBase(topic, kafkaServers) // Load to HBase after producing
    TimeUnit.SECONDS.sleep(5) // Wait 5 seconds before producing again
  }

} // <-- Ensure there's a closing brace here
