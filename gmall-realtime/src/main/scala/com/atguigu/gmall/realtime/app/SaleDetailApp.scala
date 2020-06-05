package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.bean.{OrderDetail, OrderInfo}
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
    
    def main(args: Array[String]): Unit = {
        // 1. 读数据
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SaleDetailApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        val (orderInfoStream, orderDetailStream) = getOrderInfoAndOrderDetailStreams(ssc)
        
        orderInfoStream.print
        orderDetailStream.print
        // 2. 流join, 实现一个宽表的效果
        
        
        // 3. 把宽表的数据, 写入到es
        
        
        ssc.start()
        ssc.awaitTermination()
    }
}
