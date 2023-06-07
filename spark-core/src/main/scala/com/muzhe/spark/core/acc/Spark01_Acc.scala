package com.muzhe.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/24 024 06:53
 * @Description: TODO
 */
object Spark01_Acc {

  def main(args: Array[String]): Unit = {

    /*
     todo 累加器

     累加器
     */


    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Acc"))

    val rdd = sc.makeRDD(List(1, 2, 3, 4))

    val acc = sc.longAccumulator("Acc")

    rdd.foreach(num => {
      acc.add(num)
    })

    // 获取累加器得值
    println(acc.value)

  }

}
