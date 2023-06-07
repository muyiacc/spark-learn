package com.muzhe.spark.core.test

import java.net.ServerSocket

/**
 * @Author muyiacc
 * @Date 2023/5/18 018 20:28
 * @Description: TODO
 */
object Executor {
  def main(args: Array[String]): Unit = {
    // 启动服务器，接受数据
    val server = new ServerSocket(9999)
    println("服务器启动，等待接受数据")

    // 等待客户端的连接
    val client = server.accept()

    val is = client.getInputStream

    val i = is.read()
    println("接收到客户端发送的数据：" + i)
  }
}
