package com.allin.spark.kafka.simple
import java.util
import java.util.Properties

import org.apache.kafka.clients.producer.{
  KafkaProducer,
  ProducerConfig,
  ProducerRecord,
  RecordMetadata
}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

/**
  * kafka消息生产者
  */
object MyKafkaProducer {

  def main(args: Array[String]): Unit = {
    // kafka broker地址
    val brokers = "hadoop01:9092,hadoop02:9092,hadoop03:9092"

    val props = new Properties()
    // bootstrap.servers
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    // key.serializer
    props.put(
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer].getName
    )
    // value.serializer
    props.put(
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      classOf[StringSerializer].getName
    )

    // 创建生产者
    val producer = new KafkaProducer[String, String](props)
    // 指定topic名称
    val topic = "testTopic"

    while (true) {
      val custId = "cust" + Random.nextInt(1000);
      val values = System.currentTimeMillis() + "";
      val record = new ProducerRecord[String, String](topic, custId, values)
      val future: util.concurrent.Future[RecordMetadata] = producer.send(record)
      val metadata = future.get()
      println("send message success: " + metadata.toString)

      Thread.sleep(1000)
    }
  }
}
