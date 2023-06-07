package com.muzhe.spark.core.test

import java.net.Socket

/**
 * @Author muyiacc
 * @Date 2023/5/18 018 20:28
 * @Description: TODO
 */
object Driver {
  def main(args: Array[String]): Unit = {
    //连接服务器
    val client = new Socket("localhost",9999)
    // 向服务器发送数据
    val os = client.getOutputStream

    os.write(111)
    os.flush()

    os.close()
    client.close()
  }
}
