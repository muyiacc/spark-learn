package com.muzhe.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * @Author muyiacc
 * @Date 2023/5/25 025 18:32
 * @Description: TODO 数据准备
 */

object Spark06_SQL_Hive_Test {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    // 创建spark环境
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir","/user/hive/warehouse")
      .master("local[*]")
      .appName("SQL")
      .getOrCreate()

    // 创建数据库
//    spark.sql("create database if not exist spark")
    // 使用数据库
    spark.sql("use spark")

    // 创建表
    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        |`date` string,
        |`user_id` bigint,
        |`session_id` string,
        |`page_id` bigint,
        |`action_time` string,
        |`search_keyword` string,
        |`click_category_id` bigint,
        |`click_product_id` bigint,
        |`order_category_ids` string,
        |`order_product_ids` string,
        |`pay_category_ids` string,
        |`pay_product_ids` string,
        |`city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'data/tableData//user_visit_action.txt' into table spark.user_visit_action
        |""".stripMargin)


    spark.sql(
      """
        |CREATE TABLE `product_info`(
        |`product_id` bigint,
        |`product_name` string,
        |`extend_info` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'data/tableData/product_info.txt' into table spark.product_info
        |""".stripMargin)


    spark.sql(
      """
        |CREATE TABLE `city_info`(
        |`city_id` bigint,
        |`city_name` string,
        |`area` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'data/tableData/city_info.txt' into table spark.city_info
        |""".stripMargin)

    // 查询
    spark.sql(
      """
        |select * from city_info
        |""".stripMargin).show()

  }

}
