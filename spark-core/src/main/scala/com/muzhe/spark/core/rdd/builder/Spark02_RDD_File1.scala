package com.muzhe.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/15 015 09:59
 * @Description: TODO
 */
object Spark02_RDD_File1 {

  def main(args: Array[String]): Unit = {

    // 创建spark上下文
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
//    val sparkContext = new SparkContext(sparkConf)

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("spark"))

    // 从文件中读取数据
    // testFile: 根据行进行读取
    // wholeTextFiles: 根据文件读取
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("data")

    rdd.collect.foreach(println)


    // 关闭连接
    sc.stop()
  }

}
