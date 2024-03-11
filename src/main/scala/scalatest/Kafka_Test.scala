package scalatest;

import scala.sys.process._
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Admin}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, HColumnDescriptor, HTableDescriptor}

object Kafka_Test extends App{
// Define the HBase table name
val tableName = "insurance_claims_5-3-12-98"

// Define the shell command to count the rows in the HBase table
val shellCommand = s"echo 'count {\"$tableName\"}' | hbase shell"

// Execute the shell command and capture the output
val output = shellCommand.!!

// Extract the row count from the output
val rowCount = output.trim.split(" ").head.toLong

// Print the row count
println(s"Number of rows in HBase table '$tableName': $rowCount")
}
