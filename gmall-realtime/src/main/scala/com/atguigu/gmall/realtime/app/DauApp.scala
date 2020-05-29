package com.atguigu.gmall.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.bean.StartupLog
import com.atguigu.gmall.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
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
        
        // 2. 把流中的数据封装到样例中.
        val startupLogStream: DStream[StartupLog] = sourceStream.map(log => JSON.parseObject(log, classOf[StartupLog]))
        startupLogStream.print
        // 3. 借助redis去重.
        // 3.1 从redis读到所有今天启动过的设备
        // 3.2 把已经启动过的设备过滤掉
        
        // 4. 新启动的设备写入到hbase
        
        
        ssc.start()
        ssc.awaitTermination()
    }
}

/*
时候用spark-streaming读取 启动日志, 计算日活: 只看每天的第一条启动记录

 */