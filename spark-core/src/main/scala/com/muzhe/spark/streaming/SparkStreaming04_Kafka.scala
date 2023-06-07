package com.muzhe.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author muyiacc
 * @Date 2023/6/1 001 11:02
 * @Description: TODO 连接Kafka
 */
object SparkStreaming04_Kafka {
  def main(args: Array[String]): Unit = {
    val ssc = new StreamingContext(
      new SparkConf().setMaster("local[*]").setAppName("kafka"),
      Seconds(3)
    )

    // kafka配置
    val kafkaParams = Map[String,Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092", // 连接集群
      ConsumerConfig.GROUP_ID_CONFIG -> "atguigu", // 必须配置组信息，否则会会报错
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer", // 反序列化，必须配置
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDataDS = KafkaUtils.createDirectStream[String, String](
      ssc, // 上下文环境对象
      LocationStrategies.PreferConsistent, // 位置策略，采集的节点和计算的节点如何作匹配
      ConsumerStrategies.Subscribe[String, String](Set("first"), kafkaParams) // 消费者策略
    )

    kafkaDataDS.map(_.value()).print()

    ssc.start()
    ssc.awaitTermination()
  }

}
