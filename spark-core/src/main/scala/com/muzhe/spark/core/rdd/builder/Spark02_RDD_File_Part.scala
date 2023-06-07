package com.muzhe.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/15 015 09:59
 * @Description: TODO
 */
object Spark02_RDD_File_Part {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("spark"))

    // textFile可以将文件作为数据处理的数据源，默认也可以设置分区
    // inPartitions: 最小分区数量
    // math.min(defaultParallelism, 2)
    // 可以通过第二个参数指定分区数量。
    // 但是当指定分区数量为2时，分区数量确实3，为什么呢
    /*
     源码：
     分区的计算方式
     */
    val rdd = sc.textFile("data/1.txt",2)
        rdd.saveAsTextFile("output") // 2个分区





    // 关闭连接
    sc.stop()
  }

}
