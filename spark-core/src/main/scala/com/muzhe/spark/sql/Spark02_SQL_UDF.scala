package com.muzhe.spark.sql

import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.spark.sql.SparkSession

/**
 * @Author muyiacc
 * @Date 2023/5/24 024 22:26
 * @Description: TODO
 */

object Spark02_SQL_UDF {

  def main(args: Array[String]): Unit = {

    // todo 从user.json创建一个DataFrame,
    //  再创建一个自定义函数，实现username字段前面添加 ”Username:“
    val sparkConf = new SparkConf().setMaster("local").setAppName("UDF")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    val df = spark.read.json("D:\\CodeSpace\\IdeaProjects\\sprak-learn\\data\\user.json").toDF()

    df.createOrReplaceTempView("user")

    // 错误的写法
//    val sql1 = spark.sql(
//      """
//        |select "Username:" + username
//        |from user
//        |""".stripMargin)
//    sql1.show()

//    要实现上述写法，需要实现自定义函数udf
    spark.udf.register("addNewString",(s:String)=>{"Username: " + s})

    spark.sql("select addNewString(username) from user").show()

  }



}
