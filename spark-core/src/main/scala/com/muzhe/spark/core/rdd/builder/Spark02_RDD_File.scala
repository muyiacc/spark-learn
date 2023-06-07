package com.muzhe.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/15 015 09:59
 * @Description: TODO
 */
object Spark02_RDD_File {

  def main(args: Array[String]): Unit = {

    // 创建spark上下文
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
//    val sparkContext = new SparkContext(sparkConf)

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("spark"))

    // 从文件中读取数据

    // 绝对路径
//    val rdd: RDD[String] = sc.textFile("D:\\CodeSpace\\IdeaProjects\\sprak-learn\\data\\a.txt")

    // 相对工程的路径
//    val rdd: RDD[String] = sc.textFile("data/a.txt")

    // 也可以填写目录
    val rdd: RDD[String] = sc.textFile("data")

    // 还可以填写hadoop路径
//    val rdd: RDD[String] = sc.textFile("hdfs://hadoop102/test.txt")


    rdd.collect.foreach(println)


    // 关闭连接
    sc.stop()
  }

}
