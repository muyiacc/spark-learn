package com.muzhe.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author muyiacc
 * @Date 2023/5/28 028 21:55
 * @Description: TODO 使用SparkStreaming 实现 WordCount
 */
object SparkStreaming01_WorldCount {

  def main(args: Array[String]): Unit = {
    // todo 创建sparkStreaming上下文环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // todo 逻辑
    val data = ssc.socketTextStream("localhost", 9999)

    val words = data.flatMap(_.split(" "))

    val wordOne = words.map((_, 1))

    val wordCount = wordOne.reduceByKey(_ + _)

    wordCount.print()

    // 启动采集器
    ssc.start()
    ssc.awaitTermination()
  }

}
