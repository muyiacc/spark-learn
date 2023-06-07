package com.muzhe.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 * @Author muyiacc
 * @Date 2023/6/1 001 08:24
 * @Description: TODO 使用队列给DS传入数据
 */
object SparkStreaming02_Queue {

  def main(args: Array[String]): Unit = {

    // 创建SparkStreaming上下文环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // 创建 RDD 队列
    val rddQueue = new mutable.Queue[RDD[Int]]

    // 创建QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue, false)

    // 处理队列中的RDD数据
    val mapStream = inputStream.map((_, 1))
    val reduceStream = mapStream.reduceByKey(_ + _)

    // 打印结果
    reduceStream.print()

    // 启动任务
    ssc.start()

    // 循环创建向RDD队列中放入RDD
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300,10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()

  }

}
