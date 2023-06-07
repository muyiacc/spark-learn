package com.muzhe.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/11 011 18:57
 * @Description: TODO
 */
object Spark01_WorldCount {

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

    // 3.2 对数据进行分组
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)

    // 3.3 对数据进行转换聚合 (hello,(hello,1))
    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    // 4. 采集结果打印输出
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    // 5. 关闭连接
    sc.stop()

  }

}
