package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.bean.OrderInfo
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/6/1 9:54
 */
object OrderApp {
    def main(args: Array[String]): Unit = {
        val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 1. 先消费数据
        val sourceStream: DStream[String] = MyKafkaUtil.getKafkaStream(ssc, Constant.ORDER_INFO_TOPIC)
        
        // 2. 对数据做封装
        // {"payment_way":"2","delivery_address":"OcUqVHwpgNxVoFlqTxJO","consignee":"sBFuiJ","create_time":"2020-06-01 07:41:29","order_comment":"ZIoyAeWtftFSzSQAdScI","expire_time":"","order_status":"1","out_trade_no":"2191129826","tracking_no":"","total_amount":"535.0","user_id":"4","img_url":"","province_id":"1","consignee_tel":"13611093173","trade_body":"","id":"18","parent_order_id":"","operate_time":""}
        //{"payment_way":"2","delivery_address":"ZTvwXywQNtFTDjkxKhJI","consignee":"lMeiCp","create_time":"2020-06-01 08:43:32","order_comment":"wkLZNRWhUbxlMcbXJCIK","expire_time":"","order_status":"1","out_trade_no":"5661057147","tracking_no":"","total_amount":"612.0","user_id":"5","img_url":"","province_id":"5","consignee_tel":"13828526564","trade_body":"","id":"17","parent_order_id":"","operate_time":""}
        // json的两种动作: 解析:  json字符串=>java对象   序列化: java对象  =>json字符串
        val orderInfoStream = sourceStream.map(s => JSON.parseObject(s, classOf[OrderInfo]))
        // 3. 把订单明细写入到hbase(phoenix)
        import org.apache.phoenix.spark._
        orderInfoStream.foreachRDD(rdd => {
            rdd.saveToPhoenix(
                "GMALL_ORDER_INFO1128",
                Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
                zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
            )
            /*
            1. 如果是使用分布式数据集的保存方法, 就不用考虑分区
            2. 如果是自己单独去连接外部存储, 则需要按分区来写.
             */
            
        })
        orderInfoStream.print
        
        ssc.start()
        ssc.awaitTermination()
        
    }
}
