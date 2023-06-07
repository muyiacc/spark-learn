package com.muzhe.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/20 020 16:23
 * @Description: TODO
 */
object Spark05_RDD_Operator_Transform_Glom {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    // todo 算子 - glom
    // todo 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)

    val mapRDD = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    val glomRDD = mapRDD.glom()

    glomRDD.collect().foreach(println)

    sc.stop()

  }

}
