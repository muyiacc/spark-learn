package com.muzhe.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/15 015 09:59
 * @Description: TODO
 */
object Spark01_RDD_Memory_Part1 {

  def main(args: Array[String]): Unit = {

    // todo 设置spark上下文环境的 spark.default.parallelism

    // 创建spark上下文
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")

    // 指定默认分区
    sparkConf.set("spark.default.parallelism","5")

    val sc = new SparkContext(sparkConf)


    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    rdd.saveAsTextFile("output3")


    // 根据输出文件可以看到5个 part


    // 关闭连接
    sc.stop()
  }

}
