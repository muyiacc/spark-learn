package com.muzhe.spark.sql

import org.apache.spark.sql.SparkSession

/**
 * @Author muyiacc
 * @Date 2023/5/25 025 18:32
 * @Description: TODO
 */
object Spark05_SQL_Hive {

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

    // todo 要想在spark中连接hive
    //  1.导入mysql的jdbc驱动
    //  2.把hive-site.xml、hdfs-site.xml、core-site.xml复制到resources目录下
//    spark.sql("show tables").show()
    spark.sql("select * from stu").show()

  }

}
