package com.atguigu.canal

import java.net.{InetSocketAddress, SocketAddress}

import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.Message

/**
 * Author atguigu
 * Date 2020/5/30 15:29
 */
object CanalClient {
    def main(args: Array[String]): Unit = {
        // 1. 连接到canal服务器
        // 1.1 canal服务器的地址  canal服务器的端口号
        val address = new InetSocketAddress("hadoop102", 11111)
        val connector: CanalConnector = CanalConnectors.newSingleConnector(address, "example", "", "")
        // 1.2 连接到canal
        connector.connect()
        // 2. 订阅你要处理的具体表 gmall1128下所有的表
        connector.subscribe("gmall1128.*")
        
        // 3. 读取数据, 解析
        while (true) {
            // 一致监听mysql数据变化, 所以这个地方不挺
            // 100表示最多一次拉取由于100条sql导致的数据的变化
            val msg: Message = connector.get(100)
        }
    }
}
