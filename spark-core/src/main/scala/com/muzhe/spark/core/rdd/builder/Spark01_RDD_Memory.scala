package com.muzhe.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/15 015 09:59
 * @Description: TODO
 */
object Spark01_RDD_Memory {

  def main(args: Array[String]): Unit = {

    // 创建spark上下文
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
//    val sparkContext = new SparkContext(sparkConf)

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("spark"))

    val list = List(1,2,3,4,5)

    val rdd = sc.parallelize(list)
    // makeRDD底层就是调用了parallelize
    val rdd1 = sc.makeRDD(list)

    rdd.collect().foreach(println)

    rdd1.collect().foreach(println)


    // 关闭连接
    sc.stop()
  }

}
