package scalatest

import org.apache.hadoop.hbase.client.{ConnectionFactory, Scan, ResultScanner}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable

object HBase_Test {
  def main(args: Array[String]): Unit = {
    println("#####################################################")
    println("The Program is running..")
    println("#####################################################")

    println("Connecting to HBase...")

    // HBase configuration
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", "ip-172-31-3-80.eu-west-2.compute.internal,ip-172-31-5-217.eu-west-2.compute.internal,ip-172-31-9-237.eu-west-2.compute.internal")
    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")

    val connection = ConnectionFactory.createConnection(hbaseConf)
    val tableName = TableName.valueOf("Insurance_Claim_11_3_11h_50")
    val table = connection.getTable(tableName)

    // Scan the table to count the rows and check for duplicates
    val scan = new Scan()
    val scanner: ResultScanner = table.getScanner(scan)
    var rowCount: Int = 0
    val rowKeysSet: mutable.Set[String] = mutable.Set.empty

    try {
      println("Counting rows and checking for duplicates...")
      val iterator = scanner.iterator()
      while (iterator.hasNext) {
        val result = iterator.next()
        if (!result.isEmpty) {
          rowCount += 1

          // Check for duplicate row keys
          val rowKey = Bytes.toString(result.getRow)
          if (rowKeysSet.contains(rowKey)) {
            println(s"Duplicate row found with row key: $rowKey")
          } else {
            rowKeysSet.add(rowKey)
          }

          // Check for null values and data type mismatches in each cell of the row
          result.listCells().forEach { cell =>
            val columnFamily = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
            val qualifier = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
            val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)

            // Check for null values
            if (value == null || value.isEmpty) {
              println(s"Null value found in row $rowCount, column family: $columnFamily, qualifier: $qualifier")
            }

            // Check for data type mismatches (you can customize this part based on your specific data types)
            // For example, if you expect an integer value in a certain column, you can check if the value can be parsed as an integer
            // if (isNotExpectedDataType(value, expectedDataType)) {
            //   println(s"Data type mismatch found in row $rowCount, column family: $columnFamily, qualifier: $qualifier")
            // }
          }
        }
      }
    } finally {
      scanner.close()
    }

    println("#####################################################")
    println(s"Total number of rows in the table: $rowCount")
    println("#####################################################")

    // Close HBase connection
    connection.close()
  }
}
