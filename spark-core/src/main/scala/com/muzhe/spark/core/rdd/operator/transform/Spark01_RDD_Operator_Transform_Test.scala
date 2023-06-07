package com.muzhe.spark.core.rdd.operator.transform

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/20 020 16:23
 * @Description: TODO
 */
object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Operator"))

    val rdd = sc.textFile("data/apache.log")

    val mapRDD = rdd.map(
      line => {
        val data = line.split(" ")
        data(6)
      }
    )

    mapRDD.collect().foreach(println)


    sc.stop()
  }

}
