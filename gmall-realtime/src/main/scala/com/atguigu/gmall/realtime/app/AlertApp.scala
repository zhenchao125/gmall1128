package com.atguigu.gmall.realtime.app


import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.bean.{AlertInfo, EventLog}
import com.atguigu.gmall.realtime.util.{ESUtil, MyKafkaUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/6/1 16:19
 */
object AlertApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("AlertApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 1. 消费事件日志
        val sourceStream: DStream[String] = MyKafkaUtil
            .getKafkaStream(ssc, Constant.EVENT_TOPIC)
            .window(Minutes(5), Seconds(6))
        
        // 2. 数据封装
        val eventLogStream: DStream[EventLog] = sourceStream
            .map(log => JSON.parseObject(log, classOf[EventLog]))
        // 3. 分析处理
        /*
        同一设备  -> 按照mid分组
        5分钟内 每6s统计一次  -> window  窗口的长度: 5分钟, 窗口的步长: 6s
        三次及以上用不同账号登录 -> 统计登录的账号的数
        并领取优惠劵 -> 把其他的行为过滤掉, 浏览商品的行为得留下来
         */
        // 3.1 按照设备id分组
        val eventLogGroupedStream: DStream[(String, Iterable[EventLog])] = eventLogStream
            .map(eventLog => (eventLog.mid, eventLog))
            .groupByKey
        // 3.2 产生预警信息
        val alertInfoStream = eventLogGroupedStream.map {
            case (mid, eventLogIt) =>
                // 保存5分钟内登陆的领取优惠券所有用户(建立向es写数据, 用的是java客户端, 不支持sclaa的集合, 所以, 使用java的Set)
                val uidSet: java.util.HashSet[String] = new java.util.HashSet[String]()
                // 存储5分钟内所有的事件类型
                val eventList: java.util.ArrayList[String] = new java.util.ArrayList[String]()
                // 存储领取优惠券的那些商品id
                val itemSet: java.util.HashSet[String] = new java.util.HashSet[String]()
                // 是否浏览过商品. 默认没有
                var isClickItem = false
                import scala.util.control.Breaks._
                breakable {
                    for (event <- eventLogIt) {
                        eventList.add(event.eventId)
                        
                        // 如果是优惠券就, 就uidSet 添加进去用户id
                        event.eventId match {
                            case "coupon" =>
                                uidSet.add(event.uid) // 领取优惠券的用户
                                itemSet.add(event.itemId) // 优惠券对应的商品id
                            case "clickItem" => // 如果5分种内有浏览商品, 则不产生预警. 循环提前结束.
                                isClickItem = true
                                break
                            case _ => // 其他事件不坐任何处理
                        }
                    }
                }
                // 返回预警信息.
                // (是否产生预警信息(boolean),   预警信息的封装 )
                (!isClickItem && uidSet.size() >= 3, AlertInfo(mid, uidSet, itemSet, eventList, System.currentTimeMillis()))
        }
        
        // 数据写入到es, 讲完es之后再回来完成!
        alertInfoStream
            .filter(_._1) // 把需要预警过滤出来
            .map(_._2) // 只保留预警信息
            .foreachRDD(rdd => {
                /*rdd.foreachPartition((it: Iterator[AlertInfo]) => {
                    // 同一设备，每分钟只记录一次预警。-> 靠es来保证. (spark-streaming不处理)
                    // 如果id相同, 后面的会覆盖前面的!!!  mid_ + 分钟数
                    val sources = it
                        .map(info => (info.mid + "_" + info.ts / 1000 / 60, info))
                    ESUtil.insertBulk("gmall_coupon_alert1128", sources)
                })*/
                import ESUtil._
                rdd.saveToES("gmall_coupon_alert1128")
            })
        alertInfoStream.print(10000)
        ssc.start()
        ssc.awaitTermination()
        
    }
}

/*
需求：
同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵, 每6s统计一次，
并且在登录到领劵过程中没有浏览商品。同时达到以上要求则产生一条预警日志。
同一设备，每分钟只记录一次预警。

-----
同一设备  -> 按照mid分组
5分钟内 每6s统计一次  -> window  窗口的长度: 5分钟, 窗口的步长: 6s
三次及以上用不同账号登录 -> 统计登录的账号的数
并领取优惠劵 -> 把其他的行为过滤掉, 浏览商品的行为得留下来

同时达到以上要求则产生一条预警日志 -> 日志信息写到es中
同一设备，每分钟只记录一次预警。-> 靠es来保证. (spark-streaming不处理)

 


 */