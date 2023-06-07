package com.muzhe.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/20 020 16:23
 * @Description: TODO
 */
object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    // todo map - 算子 - mapPartitions
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    val mapRDD = rdd.mapPartitions(
      iter => {
        println("=====")
        iter.map(_ * 2)
      }
    )

    mapRDD.collect().foreach(println)

    sc.stop()

  }

}
