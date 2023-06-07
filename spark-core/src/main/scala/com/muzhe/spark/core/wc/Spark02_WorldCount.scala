package com.muzhe.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/11 011 18:57
 * @Description: TODO
 */
object Spark02_WorldCount {

  def main(args: Array[String]): Unit = {
    // todo WorldCount案例

    // 1. 创建spark上下文环境对象
    // 配置sparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("WorldCount")
    val sc: SparkContext = new SparkContext(sparkConf)

    // 2. 读取文件
    val lines: RDD[String] = sc.textFile("data")

    // 3. 对数据进行处理
    // 3.1 对读取的数据进行扁平化
    val words: RDD[String] = lines.flatMap(
      word => {
        word.split(" ")
      }
    )

    // 3.2 对数据进行结构转换
    val wordToOne = words.map(word => (word, 1))

    // 3.3 对数据进行分组
    val wordGroup = wordToOne.groupBy(_._1)

    // 3.4 对分组后的数据进行聚合
    //  RDD[(String, Iterable[(String, Int)])]
    val wordToCount = wordGroup.map{
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }

    // 4. 采集结果打印输出
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    // 5. 关闭连接
    sc.stop()

  }

}
