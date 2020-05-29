package com.atguigu.gmall.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall.common.Constant
import com.atguigu.gmall.realtime.bean.StartupLog
import com.atguigu.gmall.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

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
        val startupLogStream: DStream[StartupLog] = sourceStream
            .map(log => JSON.parseObject(log, classOf[StartupLog]))
        // 3. 借助redis去重.
        // 3.1 从redis读到所有今天启动过的设备(读redis的数据)
        val filterStartupStream = startupLogStream.transform(rdd => {
            // 对rdd进行整体去重. 不能按照分区进行去重
            // 连接redis, 读取数据. 其实是在driver中,获客户端, 然后获取所有已经启动的设备
            val client: Jedis = RedisUtil.getClient
            val mids = client.smembers(Constant.STARTUP_TOPIC + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
            client.close()
            //把已经启动的过滤掉. 值留下来没有启动过的
            // 提高集合使用效率, 把集合做广播变量
            val midsBC = ssc.sparkContext.broadcast(mids)
            // rdd中只有新启动的设备
            // 3.2 把已经启动过的设备过滤掉
            rdd
                .filter(startupLog => !midsBC.value.contains(startupLog.mid))
                // 原因是因为, 如果一个mid第一批次启动的时候, 有多次启动行为的过滤
                .map(log => (log.mid, log))
                .groupByKey
                .map {
                    //                    case (mid, logIt) =>  logIt.toList.sortBy(_.ts).head
                    case (mid, logIt) => logIt.toList.minBy(_.ts) // 排序取最小
                }
        })
        filterStartupStream.foreachRDD(rdd => {
            // 3.3 把新启动的设备id写入到redis
            // 写法1: 把rdd中, 所有的mid拉取到驱动端, 一次性写入
            // 写法2: 每个分区向外写
            rdd.foreachPartition(startupLogs => {
                val client: Jedis = RedisUtil.getClient
                
                startupLogs.foreach(log => {
                    client.sadd(Constant.STARTUP_TOPIC + ":" + log.logDate, log.mid)
                })
                client.close()
            })
            // 4. 新启动的设备写入到hbase, 通过phoenix
            import org.apache.phoenix.spark._
            rdd.saveToPhoenix("GMALL_DAU1128",
                Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "TS", "LOGDATE", "LOGHOUR"),
                zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
            )
        })
        
        
        ssc.start()
        ssc.awaitTermination()
    }
}

/*
时候用spark-streaming读取 启动日志, 计算日活: 只看每天的第一条启动记录

存储已经启动的设备的id, 用set

key                         value
"mid:" + day                set  mid_1 mid_2 ...

----

hbase存储的是日活的明细: 设备每天的第一次启动的明细

 */