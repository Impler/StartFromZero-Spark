package com.allin.spark.kafka.simple
import java.util.{Collections, Properties}

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * kafka消息消费者
  */
object MyKafkaConsumer {

  def main(args: Array[String]): Unit = {

    // kafka broker地址
    val brokers = "hadoop01:9092,hadoop02:9092,hadoop03:9092"

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    // bootstrap.servers
    props.put(
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
      classOf[StringDeserializer].getName
    )
    // value.deserializer
    props.put(
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
      classOf[StringDeserializer].getName
    )

    /**
      * 指定offset位置
      * earliest: 最早的位置
      * latest: 最新的位置
      */
//    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

    // group.id 指定分组ID
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "foo")

    // 创建消息消费者
    val consumer = new KafkaConsumer[String, String](props)
    // 主题名称
    val topic = "testTopic"
    // 订阅主题
    consumer.subscribe(Collections.singleton(topic))

    while (true) {
      val msgs = consumer.poll(1000)
      val iter = msgs.iterator()
      while (iter.hasNext) {
        val msg = iter.next()
        println("poll mseeage: " + msg)
      }
    }
  }
}
