// Define a function to process each cell
def processCell(cell: org.apache.hadoop.hbase.Cell, rowCount: Int): Unit = {
  try {
    val columnFamily = Bytes.toString(cell.getFamilyArray, cell.getFamilyOffset, cell.getFamilyLength)
    val qualifier = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
    val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)

    // Process the extracted data
    // Check for null values
    if (value == null || value.isEmpty) {
      println(s"Null value found in row $rowCount, column family: $columnFamily, qualifier: $qualifier")
    }
  } catch {
    case ex: Exception =>
      println(s"Error processing cell: ${ex.getMessage}")
  }
}

// Scan the table to count the rows and check for duplicates
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

      // Process each cell in the row
      result.listCells().forEach(cell => processCell(cell, rowCount))
    }
  }
} finally {
  scanner.close()
}
