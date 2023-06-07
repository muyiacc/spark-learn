package com.muzhe.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author muyiacc
 * @Date 2023/6/1 001 12:41
 * @Description: TODO Join
 */
object SparkStreaming06_State_Join {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("State")
    val ssc = new StreamingContext(sparkConf,Seconds(5))


    val socketData1 = ssc.socketTextStream("localhost", 8888)
    val socketData2 = ssc.socketTextStream("localhost", 9999)

    val wordOne1 = socketData1.flatMap(_.split(" ")).map((_, 1))
    val wordOne2 = socketData1.flatMap(_.split(" ")).map((_, 2))

    val newDS = wordOne1.join(wordOne2)

    newDS.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
