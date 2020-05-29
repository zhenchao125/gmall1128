package com.atguigu.gmall.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * Author atguigu
 * Date 2020/5/29 9:59
 */
object RedisUtil {
    val host = "hadoop102"
    val port = 6379
    
    val conf: JedisPoolConfig = new JedisPoolConfig
    conf.setMaxTotal(100)
    conf.setMaxIdle(40)
    conf.setMinIdle(10)
    conf.setBlockWhenExhausted(true) // 忙碌的时候是否等待
    conf.setMaxWaitMillis(1000 * 60) // 最大等待时间
    conf.setTestOnBorrow(true) // 取客户端的时候, 是否做测试
    conf.setTestOnReturn(true)
    conf.setTestOnCreate(true)
    val pool = new JedisPool(conf, host, port)
    
    
    def main(args: Array[String]): Unit = {
        /*// 如何连接redis
        
        // 1. 先获取一个redis客户端
        val client = new Jedis(host, 6379, 120 * 1000)
        // 2. 读取需要的数据
        //val v: String = client.get("k1")
        val set1: util.Set[String] = client.smembers("set1")
        println(set1)
        
        // 3. 关闭客户端
        client.close()*/
        
        
        val clinet: Jedis = pool.getResource
        println(clinet.get("k1"))
        
        clinet.close() // 不是真的close而是把连接还给连接池
        
        
    }
    
    
    def getClient: Jedis = pool.getResource
}
