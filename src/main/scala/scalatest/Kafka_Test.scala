package scalatest

import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Admin}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, HColumnDescriptor, HTableDescriptor}
import scala.sys.process._

object Kafka_Test extends App {
  //####################################################
  // Parameters for the script
  val url = "http://18.133.73.36:5002/insurance_claims1" // API URL to fetch the JSON data
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
  val urlRowCount = jsonData.count()
  jsonData.show() // Display the DataFrame contents

  // Kafka servers configuration
  val kafkaServers =
    "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:909
