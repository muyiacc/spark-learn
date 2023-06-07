package com.muzhe.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/20 020 16:23
 * @Description: TODO
 */
object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)

    val mapRDD = rdd.map(
      num => {
        println("======" + num)
        num
      }
    )

    val mapRDD1 = mapRDD.map(
      num => {
        println("######" + num)
        num
      }
    )

    mapRDD1.collect()

  }

}
