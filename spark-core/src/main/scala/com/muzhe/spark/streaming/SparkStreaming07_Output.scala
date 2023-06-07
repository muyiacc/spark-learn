package com.muzhe.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author muyiacc
 * @Date 2023/6/1 001 12:41
 * @Description: TODO 输出
 */
object SparkStreaming07_Output {
  def main(args: Array[String]): Unit = {

    /*
    输出操作指定了流数据进行的动作，比如打印，存在到文件中，
    和RDD惰性求值相似，一个DS及其派生的DS没有输出操作，那么他们将不会被执行
    但是，如果SparkStreamContext中没有输出操作将会报错

    常用方法有：

    ➢ print()：在运行流程序的驱动结点上打印 DStream 中每一批次数据的最开始 10 个元素。这
    用于开发和调试。在 Python API 中，同样的操作叫 print()。
    ➢ saveAsTextFiles(prefix, [suffix])：以 text 文件形式存储这个 DStream 的内容。每一批次的存
    储文件名基于参数中的 prefix 和 suffix。”prefix-Time_IN_MS[.suffix]”。
    ➢ saveAsObjectFiles(prefix, [suffix])：以 Java 对象序列化的方式将 Stream 中的数据保存为
    SequenceFiles . 每一批次的存储文件名基于参数中的为"prefix-TIME_IN_MS[.suffix]". Python中目前不可用。
    ➢ saveAsHadoopFiles(prefix, [suffix])：将 Stream 中的数据保存为 Hadoop files. 每一批次的存
    储文件名基于参数中的为"prefix-TIME_IN_MS[.suffix]"。Python API 中目前不可用。
    ➢ （重要）foreachRDD(func)：这是最通用的输出操作，即将函数 func 用于产生于 stream 的每一个
    RDD。其中参数传入的函数 func 应该实现将每一个 RDD 中数据推送到外部系统，如将RDD 存入文件或者通过网络将其写入数据库。
     */

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Ouput")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    val data = ssc.socketTextStream("localhost", 9999)

    // 执行打印操作，输出到控制带
//    data.print()

    // 存储到文件中,默认创建的目录在工程目录下，不灵活，无法指定目录
//    data.saveAsTextFiles("saveAsTextFiles",".txt")

    // 针对上面这个问题，可以使用foreachRDD，这是最通用的方法
    data.foreachRDD(
      rdd => {
        rdd.saveAsTextFile("./output")
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
