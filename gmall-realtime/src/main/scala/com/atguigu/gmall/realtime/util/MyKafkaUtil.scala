package com.atguigu.gmall.realtime.util

import com.atguigu.gmall.common.util.PropertyUtil
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Author atguigu
 * Date 2020/5/27 16:20
 */
object MyKafkaUtil {
    
    val params = Map[String, String](
        /*"bootstrap.servers" -> "",
        "group.id" -> ""*/
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropertyUtil.getProperty("config.properties", "kafka.servers"),
        ConsumerConfig.GROUP_ID_CONFIG -> PropertyUtil.getProperty("config.properties", "kafka.group.id")
    
    )
    
    def getKafkaStream(ssc: StreamingContext, topic: String) = {
        
        KafkaUtils
            .createDirectStream[String, String, StringDecoder, StringDecoder](
                ssc,
                params,
                Set(topic))
            .map(_._2)
        
    }
    
}
