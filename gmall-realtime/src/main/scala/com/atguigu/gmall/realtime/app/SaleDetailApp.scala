package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

/**
 * Author atguigu
 * Date 2020/6/5 10:38
 */
object SaleDetailApp {
    
    /**
     * 获取order_info和order_detail流
     *
     * @param ssc
     */
    def getOrderInfoAndOrderDetailStreams(ssc: StreamingContext) = {
        val orderInfoStream = MyKafkaUtil
            .getKafkaStream(ssc, Constant.ORDER_INFO_TOPIC)
            .map(line => JSON.parseObject(line, classOf[OrderInfo]))
        
        val orderDetailStream = MyKafkaUtil
            .getKafkaStream(ssc, Constant.ORDER_DETAIL_TOPIC)
            .map(line => JSON.parseObject(line, classOf[OrderDetail]))
        
        (orderInfoStream, orderDetailStream)
    }
    
    /**
     * 把指定的内容写入到redis
     *
     * @param value
     * @param client
     * @param timeout
     */
    def saveToRedis(key: String, value: AnyRef, client: Jedis, timeout: Int) = {
        import org.json4s.DefaultFormats
        val content = Serialization.write(value)(DefaultFormats)
        client.setex(key, timeout, content)
    }
    
    /**
     * 缓存order_info
     *
     * @param orderInfo
     * @param client
     * @return
     */
    def cacheOrderInfo(orderInfo: OrderInfo, client: Jedis) = {
        
        saveToRedis("order_info:" + orderInfo.id, orderInfo, client, 30 * 60)
    }
    
    /**
     * 缓存orderDetail
     *
     * @param orderDetail
     * @param client
     * @return
     */
    def cacheOrderDetail(orderDetail: OrderDetail, client: Jedis) = {
        saveToRedis("order_detail:" + orderDetail.order_id + ":" + orderDetail.id, orderDetail, client, 30 * 60)
    }
    
    /**
     * join 参数传递的两个流
     * 返回join后的流
     *
     * @param orderInfoStream
     * @param orderDetailStream
     */
    def fullJoin(orderInfoStream: DStream[OrderInfo], orderDetailStream: DStream[OrderDetail]) = {
        val orderIdAndOrderInfo: DStream[(String, OrderInfo)] =
            orderInfoStream.map(info => (info.id, info))
        val orderIdAndOrderDetail: DStream[(String, OrderDetail)] =
            orderDetailStream.map(info => (info.order_id, info))
        
        orderIdAndOrderInfo
            .fullOuterJoin(orderIdAndOrderDetail)
            .mapPartitions((it: Iterator[(String, (Option[OrderInfo], Option[OrderDetail]))]) => {
                // 获取redis客户端
                val client: Jedis = RedisUtil.getClient
                // 读写操作
                val result: Iterator[SaleDetail] = it.flatMap {
                    // order_info有数据, order_detail有数据
                    case (orderId, (Some(orderInfo), Some(orderDetail))) =>
                        println("Some(orderInfo)   Some(orderDetail)")
                        // 1. 把order_info信息写入到缓存(因为order_detail信息有部分信息可能迟到)
                        cacheOrderInfo(orderInfo, client)
                        // 2. 把信息join到一起(其实就是放入一个样例类中)  (缺少用户信息, 后面再专门补充)
                        val saleDetail = SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                        // 3. 去order_detail的缓存找数据, 进行join
                        // 3.1 先获取这个order_id对应的所有的order_detail的key
                        import scala.collection.JavaConversions._
                        val keys: List[String] = client.keys("order_detail:" + orderInfo.id + ":*").toList // 转成scala集合
                        val saleDetails: List[SaleDetail] = keys.map(key => {
                            val orderDetail: OrderDetail = JSON.parseObject(client.get(key), classOf[OrderDetail])
                            // 删除对应的key, 如果不删, 有可能造成数据重复
                            client.del(key)
                            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                        })
                        saleDetail :: saleDetails
                    case (orderId, (Some(orderInfo), None)) =>
                        println("Some(orderInfo), None")
                        // 1. 把order_info信息写入到缓存(因为order_detail信息有部分信息可能迟到)
                        cacheOrderInfo(orderInfo, client)
                        // 3. 去order_detail的缓存找数据, 进行join
                        // 3.1 先获取这个order_id对应的所有的order_detail的key
                        import scala.collection.JavaConversions._
                        val keys: List[String] = client.keys("order_detail:" + orderInfo.id + ":*").toList // 转成scala集合
                        val saleDetails: List[SaleDetail] = keys.map(key => {
                            val orderDetail: OrderDetail = JSON.parseObject(client.get(key), classOf[OrderDetail])
                            // 删除对应的key, 如果不删, 有可能造成数据重复
                            client.del(key)
                            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                        })
                        saleDetails
                    case (orderId, (None, Some(orderDetail))) =>
                        println("None, Some(orderDetail)")
                        // 1. 去order_info的缓存中查找
                        val orderInfoJson = client.get("order_info:" + orderDetail.order_id)
                        if (orderInfoJson == null) {
                            // 3. 如果不存在, 则order_detail缓存
                            cacheOrderDetail(orderDetail, client)
                            Nil
                        } else {
                            // 2. 如果存在, 则join
                            val orderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
                            SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail) :: Nil
                        }
                }
                
                // 关闭redis客户端
                client.close()
                
                result
            })
        
    }
    
    def main(args: Array[String]): Unit = {
        // 1. 读数据
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        val (orderInfoStream, orderDetailStream) = getOrderInfoAndOrderDetailStreams(ssc)
        // 2. 流join, 实现一个宽表的效果
        val saleDetailStream = fullJoin(orderInfoStream, orderDetailStream)
        saleDetailStream.print
        // 3. 把宽表的数据, 写入到es
        
        
        ssc.start()
        ssc.awaitTermination()
    }
}

/*
join:
    内连接, 左外, 全外
    全外
    
如何在redis中缓冲:
    缓冲的是什么数据?  缓冲的是order_info的数据, 或者order_detail
  
-----------------------
oder_info
key                        value
"order_info"               hash
                           field            value
                           id1               这行数据的json字符串
                           id2               这行数据的json字符串
好处:
    key只有一个
坏处:
    1. 查找对应order_id的信息不是太方便
    2. hash中所有的数据都是同时失效(无法定制每条数据的失效时间)
--------------------------
oder_info
key                                               value
"order_info:" + id1                                这行数据json字符 "{.....}"
"order_info:" + id2                                这行数据json字符 "{.....}"
好处:
    1. 非常方便找到需要数据
    2. 每条数据的失效时间可以定制
坏处:
    key的数量会比较多
    
--------------------------
order_detail
key                                                      value
"order_detail:" + order_id1 + ":" + id1                  这行数据json字符 "{.....}"
"order_detail:" + order_id1 + ":" + id2                 这行数据json字符 "{.....}"
"order_detail:" + order_id1 + ":" + id3                 这行数据json字符 "{.....}"
 */




















