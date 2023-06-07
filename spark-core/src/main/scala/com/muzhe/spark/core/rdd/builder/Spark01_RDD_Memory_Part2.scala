package com.muzhe.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/15 015 09:59
 * @Description: TODO
 */
object Spark01_RDD_Memory_Part2 {

  def main(args: Array[String]): Unit = {

    // todo 设置spark上下文环境的 spark.default.parallelism

    // 创建spark上下文
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,5),2)
    rdd.saveAsTextFile("output")


    // 关闭连接
    sc.stop()
  }

}
