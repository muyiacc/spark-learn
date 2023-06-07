package com.muzhe.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/11 011 22:03
 * @Description: TODO
 */
object Spark04_wc {
  def main(args: Array[String]): Unit = {

    // 统计字数
    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WordCount"))
    sc.textFile("data/a.txt")
      .flatMap(_.split(" "))
      .map(word => (word,1))
      .reduceByKey(_ + _)
      .collect()
      .foreach(println)
  }

}
