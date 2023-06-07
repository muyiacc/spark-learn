package com.muzhe.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author muyiacc
 * @Date 2023/5/24 024 19:14
 * @Description: TODO
 */


object Spark01_SQL_Basic {

  def main(args: Array[String]): Unit = {

    // todo 创建环境
    val sparkConf = new SparkConf().setMaster("local").setAppName("Spark01_SQL_Basic")
    val sc = new SparkContext(sparkConf)
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    // todo 执行操作

    // todo DataFrame
    // 通过文件创建DataFrame
    val userJsonPath = "D:\\CodeSpace\\IdeaProjects\\sprak-learn\\data\\user.json"
    val df = spark.read.json(userJsonPath)

    // 创建一个临时表
    df.createTempView("user")

    // 查询
    // 通过sql查询
    //    val sqlDF = spark.sql("select * from user")
    //    sqlDF.show()

    // 通过DSL语言查询
    //    df.select("id","username","age").show

    // 上面创建的临时表的作用域是一个sparkSession
    // 使用spark.newSession()表示在一个新的Session
    //    val sqlDF = spark.newSession().sql("select * from user")
    //    sqlDF.show() // Table or view not found: user;
    // 创建一个全局表，让不同的sparkSession也可以访问
    //    df.createGlobalTempView("emp")
    //    // 查询表时需要在表前添加 global_temp
    //    val sqlDF = spark.newSession().sql("select * from global_temp.emp")
    //    sqlDF.show()

    // 查询一个列
    //    df.select("username").show()

    // 对查询的结果进行额外的操作
    // 例如 ： 对查询的 age+1
    //    df.select($"age" + 1).show()
    // 可使用 '+字段名 单引号简化上述操作
    //    df.select('age+1).show()

    // 查询姓名和 年龄+1,
    // 需要在每一个列加$或者'
    //    df.select($"username",$"age"+1).show()
    //    df.select('username,'age+1).show()


    //todo RDD => DataFrame
    // 使用 toDF()
    //    val rdd = sc.makeRDD(List(1, 2, 3, 4, 5))
    //    val rdf = rdd.toDF()
    //    rdf.show()

    //todo DataFrame => RDD
    //    val rdd = df.rdd
    //    rdd.collect().foreach(println)


    // todo DataSet

    // 创建DataSet
    // 从样例类
    val caseClassDS = Seq(Person("xiaohua", 19), Person("xiaofang", 18)).toDS()
//    caseClassDS.show()

    // 从基本数据类型创建
    val ds = Seq(1, 2, 3, 4).toDS()
//    ds.show()

    // 实际运用中，很少序列转换成创建DataSet，更多的是从RDD转换成DataSet

    // todo RDD => DataSet
    val ds1 = sc.makeRDD(List(("xiaofang", 20), ("xiaohau", 29))).map(t => {
      Person(t._1, t._2)
    }).toDS()

//    ds1.show()

    // todo DataSet => RDD
    val rdd = ds1.rdd
//    rdd.collect().foreach(println)


    // todo DataFrame => DataSet
//    val scDF = sc.makeRDD(List(("xiaofang", 20), ("xiaohau", 29))).map(t => {
//      Person(t._1, t._2)
//    }).toDF()
//
//    val scDS = scDF.as[Person]
//
//    scDS.show()


    // todo DataSet => DataFrame
    val scDS = sc.makeRDD(List(("xiaofang", 20), ("xiaohua", 29))).map(t => {
      Person(t._1, t._2)
    }).toDS()

    val scDF = scDS.toDF()

    scDF.show()


      // todo 关闭连接
    spark.stop()
  }

  // 样例类
  case class Person(name: String, age: Int)
}
