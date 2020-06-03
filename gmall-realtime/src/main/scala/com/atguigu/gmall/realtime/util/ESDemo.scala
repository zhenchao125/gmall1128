package com.atguigu.gmall.realtime.util

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.Index

/**
 * Author atguigu
 * Date 2020/6/3 13:58
 */
object ESDemo {
    def main(args: Array[String]): Unit = {
        // 先向es写数据
        
        // 1. 先获取一个es的客户端
        // 1.1 创建一个客户端工厂
        val factory = new JestClientFactory
        // 1.1.1 给工厂设置es的相关参数
        val esUrl = "http://hadoop102:8300" //注意换成自己的端口(9200)
        val config = new HttpClientConfig.Builder(esUrl)
            .maxTotalConnection(100)  // 允许的最多客户端的个数
            .connTimeout(10000)   // 连接es的超时时间
            .readTimeout(10000) // 读取数据的超时时间
            .multiThreaded(true)
            .build()
        factory.setHttpClientConfig(config)
        // 1.2 通过客户端工厂得到客户端
        val client: JestClient = factory.getObject
        // 2. 执行写的操作就行了
        // 2.1 source: 吧表示要插入的数据
        /*val source =
        """
          |{
          |  "name": "zhangsan",
          |  "age":20
          |}
          |""".stripMargin*/
        val source = User("zhiling", 40)
        val action = new Index.Builder(source)
            .index("user1128")
            .`type`("_doc")
//            .id("100")
            .build()
        client.execute(action)
        
    }
}

case class User(name: String, age: Int)
/*
source:
    一般有两种非常常用
    1. json 字符串
    2. 直接使用样例类


 */