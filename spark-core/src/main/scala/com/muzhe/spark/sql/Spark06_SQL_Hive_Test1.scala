package com.muzhe.spark.sql

import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @Author muyiacc
 * @Date 2023/5/25 025 18:32
 * @Description: TODO 求各区域热门商品 Top3
 */
object Spark06_SQL_Hive_Test1 {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")
    // 创建spark环境
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("sparkSQL")
      .getOrCreate()

    import spark.implicits._

    /*
    需求分析
    ➢ 查询出来所有的点击记录，并与 city_info 表连接，得到每个城市所在的地区，与Product_info 表连接得到产品名称
    ➢ 按照地区和商品 id 分组，统计出每个商品在每个地区的总点击次数
    ➢ 每个地区内按照点击次数降序排列
    ➢ 只取前三名
    ➢ 城市备注需要自定义 UDAF 函数
     */

    /*
    功能实现
    ➢ 连接三张表的数据，获取完整的数据（只有点击）
    ➢ 将数据根据地区，商品名称分组
    ➢ 统计商品点击次数总和,取 Top3
    ➢ 实现自定义聚合函数显示备注
     */

    spark.sql("use spark")

    // 查询所有数据
    spark.sql(
      """
        | select
        |   a.*,
        |   p.product_name,
        |   c.area,
        |   c.city_name
        | from user_visit_action a
        | join product_info p on a.click_product_id = p.product_id
        | join city_info c on a.city_id = c.city_id
        | where a.click_product_id > -1
        |""".stripMargin).createOrReplaceTempView("t1")


    // 根据区域，商品进行数据聚合

    spark.udf.register("cityRemark",functions.udaf(new CityRemarkUDAF()))

    spark.sql(
      """
        | select
        |   area,
        |   product_name,
        |   count(*) as cliCnt,
        |   cityRemark(city_name) as city_remark
        | from
        |   t1
        | group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    // 区域内对点击次数进行排行
    spark.sql(
      """
        | select
        |   *,
        |   rank() over( partition by area order by cliCnt desc ) as rank
        | from
        |   t2
        |""".stripMargin).createOrReplaceTempView("t3")

    // 取前3名
    spark.sql(
      """
        | select
        |   *
        | from t3
        | where rank <= 3
        |""".stripMargin).show(false)

    spark.close()
  }

  case class Buffer(var total: Long, var cityMap: mutable.Map[String, Long])

  // 自定义聚合函数，实现城市备注
  // 1.继承Aggregator,定义泛型
  //    IN: 城市名称
  //    Buff: 【总点击数量，Map[(city,cnt),(city,cnt)]】
  //    Out: 备注信息
  // 2.重写方法
  class CityRemarkUDAF extends Aggregator[String,Buffer,String] {
    // 缓冲区的初始值
    override def zero: Buffer = {
      Buffer(0,mutable.Map[String,Long]())
    }

    // 缓存区的更新
    override def reduce(buff: Buffer, city: String): Buffer = {
      buff.total += 1
      val newCount = buff.cityMap.getOrElse(city,0L) + 1
      buff.cityMap.update(city,newCount)
      buff
    }

    // 合并缓冲区数据
    override def merge(buff1: Buffer, buff2: Buffer): Buffer = {
      buff1.total += buff2.total

      val map1 = buff1.cityMap
      val map2 = buff2.cityMap

      // 两个map的合并
      buff1.cityMap = map1.foldLeft(map2){
        case(map,(city,cnt)) => {
          val newCount = map.getOrElse(city,0L) + cnt
          map.update(city, newCount)
          map
        }
      }

      buff1
    }

    // 将统计的结果生成字符串
    override def finish(buff: Buffer): String = {
      val remarkList = ListBuffer[String]()

      val totalCnt = buff.total
      val cityMap = buff.cityMap

      val cityCntList = cityMap.toList.sortWith(
        (left,right) => {
          left._2 > right._2
        }
      ).take(2)

      var rsum = 0L
      val hasMore = cityMap.size > 2
      cityCntList.foreach{
        case (city, cnt) => {
          val r = cnt * 100 / totalCnt
          remarkList.append(s"${city} ${r}%")
          rsum += r
        }
      }

      if (hasMore){
        remarkList.append(s"其他 ${100 - rsum}%")
      }

      remarkList.mkString(", ")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

}
