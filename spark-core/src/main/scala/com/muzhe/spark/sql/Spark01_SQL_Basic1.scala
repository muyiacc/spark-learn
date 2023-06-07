package com.muzhe.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import java.util.Properties

/**
 * @Author muyiacc
 * @Date 2023/5/24 024 19:14
 * @Description: TODO
 */

// todo 从不同的数据源读取数据
object Spark01_SQL_Basic1 {
  def main(args: Array[String]): Unit = {
    // 创建环境
    val sparkConf = new SparkConf().setMaster("local").setAppName("Spark01_SQL_Basic")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // todo 加载数据
    // todo 读取 json格式
    val jsonPath = "D:\\CodeSpace\\IdeaProjects\\sprak-learn\\data\\user1.json"
    //    val jsonDF = spark.read.format("json").load(jsonPath)
    val jsonDF = spark.read.json(jsonPath)
    //    jsonDF.show()

    // todo 读取 csv格式
    val csvPath = "D:\\CodeSpace\\IdeaProjects\\sprak-learn\\data\\people.csv"
    // 方式一: 使用option
    //    val csvDF = spark.read
    //      .option("header","true") // 将第一行作为列名
    //      .option("inferSchema","true") // 自动推断类型
    //      .option("delimiter",";") // 指定分隔符
    //      .csv(csvPath)
    //    csvDF.show()

    // 方式二: 使用 format("csv")+load()
//    val csvDF = spark.read.format("csv")
//      .option("header", "true") // 将第一行作为列名
//      .option("inferSchema", "true") // 自动推断类型
//      .option("delimiter", ";") // 指定分隔符
//      .load(csvPath)
    //    csvDF.show()

    // 方式三: 使用options()
//    val csvDF = spark.read.format("csv")
//      .options(Map(("header", "true"), ("inferSchema", "true"), ("delimiter", ";"))).load(csvPath)

//    csvDF.show()

    // todo 读取 MySQL
    // 需要导入jdbc依赖

    // 方式一：通过jdbc读取
//    val url = "jdbc:mysql://localhost:3306/spark_demo"
//    val prop = new Properties()
//    prop.setProperty("user","root") // 这里连接数据库的用户名用到的是"user",而不是"username"
//    prop.setProperty("password","root")
//    spark.read.jdbc(url,"user",prop).show()

    // 方式二：通过load加载
//    spark.read.format("jdbc")
//      .option("url","jdbc:mysql://localhost:3306/spark_demo")
//      .option("driver","com.mysql.cj.jdbc.Driver")
//      .option("user","root")
//      .option("password","root")
//      .option("dbtable","user")
//      .load().show()

    // 方式三：load加载的另一种参数形式
    spark.read.format("jdbc")
      .options(Map("url"->"jdbc:mysql://localhost:3306/spark_demo","user"->"root","password"->"root",
        "driver"->"com.mysql.cj.jdbc.Driver","dbtable"->"user"))
      .load.show()
  }


}
