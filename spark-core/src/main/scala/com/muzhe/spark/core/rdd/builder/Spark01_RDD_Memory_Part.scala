package com.muzhe.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author muyiacc
 * @Date 2023/5/15 015 09:59
 * @Description: TODO
 */
object Spark01_RDD_Memory_Part {

  def main(args: Array[String]): Unit = {

    // 创建spark上下文
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("spark")
    val sc = new SparkContext(sparkConf)

    // 创建rdd
    // rdd并行度和分区
    // makeRDD可以传递第二个参数，表示分区的数量
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)

    // saveAsTextFile: 将处理的数据保存为分区文件，在项目路径下
    rdd.saveAsTextFile("output") //通过输出的output文件夹 ，可以看到有2个part，也就是2个分区

    // 也可以不传递分区数量，默认使用了defaultParallelism
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    rdd1.saveAsTextFile("output1") // 通过文件可以看出，默认分区数量为8个

    /*
    那么，这8个是怎么来的呢，通过源码进行分析
    1)
    def makeRDD[T: ClassTag](
        seq: Seq[T],
        numSlices: Int = defaultParallelism): RDD[T] = withScope {
      parallelize(seq, numSlices)
    }

    分析：makeRDD的参数defaultParallelism就是默认分区数量，点进去继续查看

    2)
    def defaultParallelism: Int = {
      assertNotStopped()
      taskScheduler.defaultParallelism
    }

    分析：返回的是taskScheduler.defaultParallelism，点击defaultParallelism继续查看

    3)
    def defaultParallelism(): Int

    分析：它在一个trite类中，这是一个抽象方法，使用idea的ctrl+h查看结构，点击TaskScheduleImpl实现类
    搜索defaultParallelism，得到  override def defaultParallelism(): Int = backend.defaultParallelism()

    4)
      override def defaultParallelism(): Int = backend.defaultParallelism()

      分析：继续点击backend.defaultParallelism()的defaultParallelism得到
        def defaultParallelism(): Int，它也在一个抽象类中，使用继承关系查看（ctrl+h）
        可以看到有两个实现类 (1) LocalScheduleBackend (2)CoarseGrainedSchedulerBackend
        因为我们创建上下文使用的local，点击LocalScheduleBackend继续查看，搜索defaultParallelism

    5)
    根据上一步结果得到
    override def defaultParallelism(): Int =
      scheduler.conf.getInt("spark.default.parallelism", totalCores)

    分析：
    scheduler.conf.getInt("spark.default.parallelism", totalCores 就是核心代码
    点击 conf 得到   val conf = sc.conf，点击 conf继续得到 private[spark] def conf: SparkConf = _conf
    结果就明显了，就是使用了SparkConf，也就是我们创建上下文时的配置
    因为我们没有指定spark.default.parallelism，默认使用的就是totalCores，totalCores就是运行环境的最大核心数，也就是当前机器的核心数 8


    todo 总结：spark上下文环境不指定spark.default.parallelism，默认使用的分区数就是当前运行环境的最大核心数
     */


    // 既然如此，我们可以再次测试，新建一个单例对象，测试



    // 关闭连接
    sc.stop()
  }

}
