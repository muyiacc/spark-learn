package com.muzhe.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author muyiacc
 * @Date 2023/6/1 001 12:41
 * @Description: TODO 滑窗 window:基于对源 Dstream窗口进行批次处理，返回一个新的DS
 */
object SparkStreaming06_State_Window_Other {
  def main(args: Array[String]): Unit = {
    /**
     * 滑窗
     * 除 window外还有其他的方法
     *
     * countByWindow()
     * reduceByWindow
     * reduceByKeyAndWindow
     * countByValueAndWindow
     * groupByKeyAndWindow
     */

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("State")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    // 使用有状态操作，需要设置检查点
    ssc.checkpoint("./cp")

    val socketData = ssc.socketTextStream("localhost", 8888)

    val wordOne = socketData.flatMap(_.split(" ")).map((_, 1))

    /*
    countByWindow(windowDuration: Duration,slideDuration: Duration)

    返回一个滑动窗口计数流中的元素个数
     */
//    val countByWindowDS = wordOne.countByWindow(Seconds(6), Seconds(3))
//    countByWindowDS.print()


    /*
    countByValueAndWindow(
          windowDuration: Duration,
          slideDuration: Duration,
          numPartitions: Int = ssc.sc.defaultParallelism)

    返回的 DStream 则包含窗口中每个值的个数
     */
//    val countByValueAndWindowDS = wordOne.countByValueAndWindow(Seconds(6), Seconds(3))
//    countByValueAndWindowDS.print()


    /*
    通过使用自定义函数整合滑动区间流元素来创建一个新的单元素流
     */
//    wordOne.reduceByWindow()

    /*
    当在一个(K,V)
    对的 DStream 上调用此函数，会返回一个新(K,V)对的 DStream，此处通过对滑动窗口中批次数
    据使用 reduce 函数来整合每个 key 的 value 值
     */
//    wordOne.reduceByKeyAndWindow()


//    wordOne.groupByKeyAndWindow()
val groupByKeyAndWindowDS = wordOne.groupByKeyAndWindow(Seconds(6), Seconds(3))
    groupByKeyAndWindowDS.print()



    ssc.start()
    ssc.awaitTermination()
  }
}
