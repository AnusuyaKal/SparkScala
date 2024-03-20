// import scala.io.Source
// import org.apache.spark.sql.SparkSession
// import org.apache.hadoop.hbase.util.Bytes
// import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Admin}
// import org.apache.hadoop.hbase.{HBaseConfiguration, TableName, HColumnDescriptor, HTableDescriptor}
// import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
// import java.util.{Collections, Properties}
// import org.apache.spark.sql.types._

// object Kafka_Test extends App {
//   val props = new Properties()
//   props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092")
//   val adminClient = AdminClient.create(props)

//   // Parameters for the script
//   val url = "http://18.133.73.36:5004/insurance_claims1"
//   val topic = "InsuranceClaims2"

//   // Initialize Spark session for DataFrame and Dataset APIs
//   val spark = SparkSession.builder.appName("API to Kafka").getOrCreate()
//   import spark.implicits._

//   // Fetch data from URL and convert it to a string
//   val result = Source.fromURL(url).mkString

//   // Read the JSON data into a DataFrame
//   val jsonData = spark.read.json(Seq(result).toDS)
//   val urlRowCount = jsonData.count()
//   jsonData.show() // Display the DataFrame contents

//   // Data Validation: Schema validation
//   // Check if the DataFrame adheres to a predefined schema
//   // val schema = StructType(Seq(
//   //   StructField("AGE", StringType, nullable = false),
//   //   StructField("BIRTH", StringType, nullable = false)
//   // ))
//   // val schemaValidation = jsonData.schema.equals(schema)
//   // println(s"Schema validation result: $schemaValidation")

//   // Kafka servers configuration
//   val kafkaServers = "ip-172-31-3-80.eu-west-2.compute.internal:9092,ip-172-31-5-217.eu-west-2.compute.internal:9092,ip-172-31-13-101.eu-west-2.compute.internal:9092,ip-172-31-9-237.eu-west-2.compute.internal:9092"

//   // Publish the DataFrame as JSON string to the specified Kafka topic
//   jsonData.selectExpr("to_json(struct(*)) AS value")
//     .write
//     .format("kafka")
//     .option("kafka.bootstrap.servers", kafkaServers)
//     .option("topic", topic)
//     .save()

//   // Read from the Kafka topic
//   val df = spark.read
//     .format("kafka")
//     .option("kafka.bootstrap.servers", kafkaServers)
//     .option("subscribe", topic)
//     .option("startingOffsets", "earliest")
//     .load()

//   // Convert Kafka messages to string for processing
//   val messages = df.selectExpr("CAST(value AS STRING) as message").as[String]

//   // Data Quality Checks: Completeness check
//   // Check if the DataFrame has non-null values for required fields
//   val completenessCheck = jsonData.na.drop().count() == jsonData.count()
//   println(s"Completeness check result: $completenessCheck")

//   // Configure HBase connection
//   val hbaseConf = HBaseConfiguration.create()
//   hbaseConf.set("hbase.zookeeper.quorum", "ip-172-31-3-80.eu-west-2.compute.internal,ip-172-31-5-217.eu-west-2.compute.internal,ip-172-31-9-237.eu-west-2.compute.internal")
//   hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
//   hbaseConf.set("zookeeper.znode.parent", "/hbase")

//   // Establish HBase connection
//   val connection = ConnectionFactory.createConnection(hbaseConf)

//   // Prepare HBase table for storing the messages
//   val tableName = TableName.valueOf(topic)
//   val columnFamilyName = "cf"
//   val admin = connection.getAdmin

//   // Check if table exists, if not, create it
//   if (!admin.tableExists(tableName)) {
//     val tableDescriptor = new HTableDescriptor(tableName)
//     val columnDescriptor = new HColumnDescriptor(columnFamilyName)
//     tableDescriptor.addFamily(columnDescriptor)
//     admin.createTable(tableDescriptor)
//     println(s"Table $tableName created.")
//   }

//   // Get the HBase table
//   val table = connection.getTable(tableName)

//   // Function to generate unique row keys for messages
//   def generateUniqueRowKey(message: String): String = {
//     val currentTime = System.currentTimeMillis()
//     val messageHash = message.hashCode
//     s"$currentTime-$messageHash"
//   }

//   // Insert each Kafka message into HBase
//   messages.collect().foreach { message =>
//     val rowKey = generateUniqueRowKey(message)
//     val put = new Put(Bytes.toBytes(rowKey))
//     put.addColumn(Bytes.toBytes(columnFamilyName), Bytes.toBytes("column"), Bytes.toBytes(message))
//     table.put(put)
//   }

//   // Data Quality Checks: Uniqueness check
//   // Check if there are any duplicate messages in the Kafka topic
//   val uniqueMessageCount = messages.distinct().count()
//   val uniquenessCheck = uniqueMessageCount == messages.count()
//   println(s"Uniqueness check result: $uniquenessCheck")

//   // Print summary of operations
//   val kafkaRowCount = df.count()
//   println(s"Number of rows in DataFrame from URL: $urlRowCount")
//   println(s"Number of rows in DataFrame from Kafka topic: $kafkaRowCount")
//   println("----------------------------")
//   println(s"It was the API  : $url")
//   println(s"Kafka Topic was : $topic")
//   println(s"HBase table is  : $tableName")
//   println("----------------------------")

// // HBase configuration
//      val hbaseConf = HBaseConfiguration.create()
//      hbaseConf.set("hbase.zookeeper.quorum", "ip-172-31-3-80.eu-west-2.compute.internal,ip-172-31-5-217.eu-west-2.compute.internal,ip-172-31-9-237.eu-west-2.compute.internal")
//      hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
//      val connection = ConnectionFactory.createConnection(hbaseConf)
//      val tableName = TableName.valueOf("Insurance_Claim_11_3_11h_50")
//      val table = connection.getTable(tableName)
// // Scan the table to count the rows
//      val scan = new Scan()
//      val scanner: ResultScanner = table.getScanner(scan)
//      var rowCount: Int = 0

//      try {
//        println("Counting rows...")
//        val iterator = scanner.iterator()
//        while (iterator.hasNext) {
//          val result = iterator.next()
//          if (!result.isEmpty) {
//            rowCount += 1
//          }
//        }
//     } finally {
//       scanner.close()
//     }
//      print("################## RESULT #####################")
//      println(s"Total number of rows in the table: $rowCount")
//      print("################## RESULT #####################")
//    }


//  }

// // Close HBase table and connection to clean up resources
// table.close()
// connection.close()
// }
