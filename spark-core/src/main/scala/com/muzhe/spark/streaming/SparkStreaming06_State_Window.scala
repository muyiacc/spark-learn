package com.muzhe.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author muyiacc
 * @Date 2023/6/1 001 12:41
 * @Description: TODO 滑窗 window:基于对源 Dstream窗口进行批次处理，返回一个新的DS
 */
object SparkStreaming06_State_Window {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("State")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    // 使用有状态操作，需要设置检查点
    ssc.checkpoint("./cp")

    val socketData = ssc.socketTextStream("localhost", 8888)

    val wordOne = socketData.flatMap(_.split(" ")).map((_, 1))


    // windowDuration: Duration, slideDuration: Duration
    // windowDuration 滑窗的时间，必须为采集周期的整数倍
    // slideDuration 滑窗移动的时间，也必须为采集周期的整数倍
    val windowDS = wordOne.window(Seconds(6), Seconds(6))

    val wordCount = windowDS.reduceByKey(_ + _)

    wordCount.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
