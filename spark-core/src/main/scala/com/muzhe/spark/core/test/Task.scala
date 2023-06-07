package com.muzhe.spark.core.test

/**
 * @Author muyiacc
 * @Date 2023/5/18 018 20:47
 * @Description: TODO
 */
class Task {

  val datas = List(1,2,3,4,5)

  val logic = (num: Int) => num *2

  // 计算
  def compute() = {
    datas.map(logic)
  }

}
