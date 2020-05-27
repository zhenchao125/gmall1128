package com.atguigu.gmall.realtime.app

import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Author atguigu
 * Date 2020/5/27 16:17
 */
object DauApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("DauApp").setMaster("local[2]")
        val ssc = new StreamingContext(conf, Seconds(3))
        // 1. 消费kafka数据
        val sourceStream = MyKafkaUtil.getKafkaStream(ssc, Constant.STARTUP_TOPIC)
        
        sourceStream.print
        
        ssc.start()
        ssc.awaitTermination()
    }
}

/*
时候用spark-streaming读取 启动日志, 计算日活: 只看每天的第一条启动记录

 */