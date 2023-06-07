package com.muzhe.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @Author muyiacc
 * @Date 2023/6/1 001 12:41
 * @Description: TODO 优雅的关闭
 */
object SparkStreaming08_Close {
  def main(args: Array[String]): Unit = {

    /**
     * 何为优雅的关闭？
     * 优雅的关闭 vs 强制关闭
     *  强制关闭： 不管任务执行如何，直接关闭连接   ssc.stop()
     *  优雅的关闭： 不再接受新的数据，执行完当前的任务再关闭
     *
     * 如何实现优雅关闭
     */

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Ouput")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val data = ssc.socketTextStream("localhost", 9999)

    data.print()

    ssc.start()

    /*
     todo 要执行优雅的关闭，当然得在 上下文环境启动之后，但是又不能在 ssc.awaitTermination之后，不然就一致被阻塞，执行不到了。

     因为在主线程中，想要关闭采集器，就得新开一个线程去关闭它
     val thread = new Thread()

     */
//    var flag = false
    new Thread(new Runnable {
      override def run(): Unit = {
        // 但是这又有一个新的问题，总不能线程开启之后就关闭了，所以得有一个状态让我们 去关闭它
        // 这个状态表示了前面代码我们不再不要它执行了，这里就用flag表示

//        if (flag){
//
//          // 当ssc本就为关闭时，就不执行了
//          val streamingContextState = ssc.getState()
//          if(streamingContextState == StreamingContextState.ACTIVE){
//            ssc.stop(true,true)
//          }
//        }
//        Thread.sleep(5000L)
//        flag = true

        // 这里用作测试，当程序执行5s后，就关闭它
        Thread.sleep(5000L)
        val streamingContextState = ssc.getState()
        if(streamingContextState == StreamingContextState.ACTIVE){
          ssc.stop(true,true)
        }
        System.exit(0)
      }
    }).start()

    ssc.awaitTermination()
  }
}
