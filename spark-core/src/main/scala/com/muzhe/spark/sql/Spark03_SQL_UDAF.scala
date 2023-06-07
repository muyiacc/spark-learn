package com.muzhe.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

/**
 * @Author muyiacc
 * @Date 2023/5/24 024 22:26
 * @Description: TODO
 */

object Spark03_SQL_UDAF {

  def main(args: Array[String]): Unit = {

    // todo 创建自定义的聚合函数
    val sparkConf = new SparkConf().setMaster("local").setAppName("UDAF")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df = spark.read.json("D:\\CodeSpace\\IdeaProjects\\sprak-learn\\data\\user.json")

    // 统计user中age的平均值
    val df1 = df.select(avg("age"))
//    df1.show()

    // todo 上述功能，如何自定义函数实现呢
    // 参考avg的写法

  }



}
