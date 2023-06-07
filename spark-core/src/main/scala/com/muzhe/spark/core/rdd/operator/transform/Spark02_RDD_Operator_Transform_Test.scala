package com.muzhe.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/20 020 16:23
 * @Description: TODO
 */
object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    // todo map - 算子 - mapPartitions
    // todo 查询每个分区的最大值
    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    val mapRDD = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )

    mapRDD.collect().foreach(println)

    sc.stop()

  }

}
