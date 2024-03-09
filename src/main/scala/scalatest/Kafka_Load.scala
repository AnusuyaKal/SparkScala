package scalatest;
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConverters._
import scala.util.Random
import scala.concurrent.duration._

object Kafka_Load {
  val kafkaConfig = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "<kafka_broker>",
    ConsumerConfig.GROUP_ID_CONFIG -> "my_group",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
  )

  val hbaseConfig: org.apache.hadoop.conf.Configuration = ??? // Define your HBase configuration here

  val topic = "Kafka1"
  val tableName = "Anu_Tab"

  def produceData(): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "<kafka_broker>")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    while (true) {
      val records = (1 to 1000).map(_ => Random.nextInt(1000))
      records.foreach { record =>
        val data = s"""{"id": $record, "data": ${Random.nextInt(100)}}"""
        producer.send(new ProducerRecord[String, String](topic, data))
      }
      Thread.sleep(5.seconds.toMillis)
    }
  }

  def consumeAndStoreData(): Unit = {
    val consumer = new KafkaConsumer[String, String](kafkaConfig.asJava)
    consumer.subscribe(List(topic).asJava)

    val connection: Connection = ConnectionFactory.createConnection(hbaseConfig)
    val table = connection.getTable(org.apache.hadoop.hbase.TableName.valueOf(tableName))

    while (true) {
      val records = consumer.poll(10.seconds.toMillis)
      records.asScala.foreach { record =>
        val data = record.value().split(", ").map(_.split(": ")(1))
        val rowKey = data(0).drop(1).init
        val put = new Put(Bytes.toBytes(rowKey))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("data"), Bytes.toBytes(data(1)))
        table.put(put)
        println(s"Data stored in HBase: $record")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val producerThread = new Thread(new Runnable {
      override def run(): Unit = produceData()
    })
    val consumerThread = new Thread(new Runnable {
      override def run(): Unit = consumeAndStoreData()
    })

    producerThread.start()
    consumerThread.start()
  }
}
