package com.muzhe.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

/**
 * @Author muyiacc
 * @Date 2023/6/1 001 08:56
 * @Description: TODO 自定义采集器
 */
object SparkStreaming03_DIYReceiver {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Receiver")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val messageDS = ssc.receiverStream(new MyReceiver())
    messageDS.print()

    ssc.start()
    ssc.awaitTermination()
  }


  /**
   * 自定义数据采集器
   * 1 继承Receiver，指定泛型，传入参数
   * 2 重写方法 onStart() onStop()
   * 3 通过SparkStreamContext上下文环境对象调用 receiverStream，在 receiverStream中传入自定的采集器对象
   */
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flag = true
    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flag){
            val message = "采集的数据为：" + new Random().nextInt(10).toString
            store(message)
            Thread.sleep(500)
          }

        }
      }).start()
    }

    override def onStop(): Unit = {
      flag = false
    }
  }
}
