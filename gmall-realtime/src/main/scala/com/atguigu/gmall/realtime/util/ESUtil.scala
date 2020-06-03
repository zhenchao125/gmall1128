package com.atguigu.gmall.realtime.util

import com.atguigu.gmall.realtime.bean.AlertInfo
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, Index}
import org.apache.spark.rdd.RDD

/**
 * Author atguigu
 * Date 2020/6/3 13:58
 */
object ESUtil {
    
    val factory = new JestClientFactory
    // 1.1.1 给工厂设置es的相关参数
    val esUrl = "http://hadoop102:8300" //注意换成自己的端口(9200)
    val config = new HttpClientConfig.Builder(esUrl)
        .maxTotalConnection(100) // 允许的最多客户端的个数
        .connTimeout(10000) // 连接es的超时时间
        .readTimeout(10000) // 读取数据的超时时间
        .multiThreaded(true)
        .build()
    factory.setHttpClientConfig(config)
    
    def main(args: Array[String]): Unit = {
        // 先向es写数据
        
        /*// 1. 先获取一个es的客户端
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
        //        // 2. 执行写的操作就行了
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
        client.execute(action)*/
        
        
        /* val source = User("fengjie", 50)
         insertSingle("user1128", source)*/
        
        val it = Iterator(("30", User("a", 10)), ("30", User("b", 20)))
        //        val it = Iterator(User("a", 10), User("b", 20))
        insertBulk("user1128", sources = it)
        
        
    }
    
    /**
     * 向es中插入单条数据
     *
     * @param index
     * @param source
     * @param id
     */
    def insertSingle(index: String, source: Object, id: String = null): Unit = {
        val client: JestClient = factory.getObject
        val action = new Index.Builder(source)
            .index(index)
            .`type`("_doc")
            .id(id) // 如果是传递的null, 则相当于没有传
            .build()
        client.execute(action)
        client.shutdownClient() // 把客户端还给工厂
    }
    
    /**
     * 批量插入
     *
     * @param index
     * @param sources
     */
    def insertBulk(index: String, sources: Iterator[Object]) = {
        val client: JestClient = factory.getObject
        val builder = new Bulk.Builder()
            .defaultIndex(index)
            .defaultType("_doc")
        // 在一个Bulk.Builder中add进去多个Action, 可以一次性交给es完成插入
        // Object   (id, object)
        sources.foreach {
            case (id: String, data) =>
                val action = new Index.Builder(data)
                    .id(id)
                    .build()
                builder.addAction(action)
            case data =>
                val action = new Index.Builder(data)
                    .build()
                builder.addAction(action)
        }
        
        client.execute(builder.build())
        client.shutdownClient()
    }
    
    implicit class RichES(rdd: RDD[AlertInfo]) {
        def saveToES(index: String): Unit = {
            rdd.foreachPartition((it: Iterator[AlertInfo]) => {
                // 同一设备，每分钟只记录一次预警。-> 靠es来保证. (spark-streaming不处理)
                // 如果id相同, 后面的会覆盖前面的!!!  mid_ + 分钟数
                val sources = it
                    .map(info => (info.mid + "_" + info.ts / 1000 / 60, info))
                ESUtil.insertBulk("gmall_coupon_alert1128", sources)
            })
        }
    }
    
}

case class User(name: String, age: Int)

/*
source:
    一般有两种非常常用
    1. json 字符串
    2. 直接使用样例类


 */