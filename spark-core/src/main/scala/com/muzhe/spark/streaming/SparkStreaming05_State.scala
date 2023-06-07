package com.muzhe.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author muyiacc
 * @Date 2023/6/1 001 12:41
 * @Description: TODO 状态 转换
 */
object SparkStreaming05_State {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("State")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    // 使用有状态操作，需要设置检查点
    ssc.checkpoint("./cp")

    val socketData = ssc.socketTextStream("localhost", 8888)

    val wordOne = socketData.map((_, 1))

    // todo 无状态操作，只对采集周期进行处理
    // 像 map()、flatMap()、filter()、reducerByKey()等都是无状态操作
//    val wordCount = wordOne.reduceByKey(_ + _)

//    wordCount.print()


    // todo 有状态操作，保留采集周期的数据，用来实现数据的统计

    val state = wordOne.updateStateByKey[Int](
      (seq:Seq[Int], buff:Option[Int]) => {
        val newCount = buff.getOrElse(0) + seq.sum
        Some(newCount)
      }
    )

    state.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
