package com.atguigu.canal

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
 * Author atguigu
 * Date 2020/5/30 16:45
 */
object MykafkaUtil {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadooop104:9092")
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    
    val producer = new KafkaProducer[String, String](props)
    
    def  send(topic: String, content: String)  = {
        producer.send(new ProducerRecord[String, String](topic, content))
    }
}
