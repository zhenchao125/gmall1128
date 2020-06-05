package com.atguigu.canal

import java.net.InetSocketAddress
import java.util
import java.util.Random

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.client.{CanalConnector, CanalConnectors}
import com.alibaba.otter.canal.protocol.CanalEntry.{EntryType, EventType, RowChange}
import com.alibaba.otter.canal.protocol.{CanalEntry, Message}
import com.atguigu.gmall.common.Constant
import com.google.protobuf.ByteString

import scala.collection.JavaConversions._

/**
 * Author atguigu
 * Date 2020/5/30 15:29
 */
object CanalClient {
    // 真正的处理数据
    def parseData(rowDataList: util.List[CanalEntry.RowData],
                  tableName: String,
                  eventType: CanalEntry.EventType) = {
        // 计算订单总额 order_info
        if (tableName == "order_info" && eventType == EventType.INSERT && rowDataList != null && rowDataList.size() > 0) {
            sendToKafka(Constant.ORDER_INFO_TOPIC, rowDataList)
        } else if (tableName == "order_detail" && eventType == EventType.INSERT && rowDataList != null && rowDataList.size() > 0) {
            sendToKafka(Constant.ORDER_DETAIL_TOPIC, rowDataList)
        }
    }
    
    private def sendToKafka(topic: String, rowDataList: util.List[CanalEntry.RowData]): Unit = {
        for (rowData <- rowDataList) {
            val result = new JSONObject()
            // 一个rowData表示一行数据, 所有列组成一个json对象, 写入到Kafka中
            val columnList: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
            for (column <- columnList) { // column 列
                val key: String = column.getName // 列名
                val value = column.getValue // 列值
                result.put(key, value)
            }
            // 把数据写入到kafka中. 用一个生产者
            // 单纯的模拟数据延迟情况
            new Thread(){
                override def run(): Unit = {
                    Thread.sleep(new Random().nextInt(1000 * 20))  // 随机延迟0-20s
                    MykafkaUtil.send(topic, result.toJSONString)
                }
            }.start()
           
        }
    }
    
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
            val entries: util.List[CanalEntry.Entry] = msg.getEntries
            if (entries != null && entries.size() > 0) {
                // 遍历拿到每个entry
                import scala.collection.JavaConversions._
                for (entry <- entries) {
                    // 处理的EntryType应该时刻RowData
                    if (entry != null && entry.hasEntryType && entry.getEntryType == EntryType.ROWDATA) {
                        // 获取storeValue. 每个entry一个
                        val storeValue: ByteString = entry.getStoreValue
                        // 每个storeVales一个RowChange
                        val rowChange: RowChange = RowChange.parseFrom(storeValue)
                        // 每个rowChange中多个RowData. 一个RowData就表示一行数据
                        val rowDataList: util.List[CanalEntry.RowData] = rowChange.getRowDatasList
                        parseData(rowDataList, entry.getHeader.getTableName, rowChange.getEventType)
                    }
                }
            } else {
                println("没有拉倒数据, 2s之后继续拉....")
                Thread.sleep(2000)
            }
        }
    }
}

/*
#################################################
## mysql serverId , v1.0.26+ will autoGen
canal.instance.mysql.slaveId=100

# enable gtid use true/false
canal.instance.gtidon=false

# position info
canal.instance.master.address=hadoop102:3306
canal.instance.master.journal.name=
canal.instance.master.position=
canal.instance.master.timestamp=
canal.instance.master.gtid=

# rds oss binlog
canal.instance.rds.accesskey=
canal.instance.rds.secretkey=
canal.instance.rds.instanceId=

# table meta tsdb info
canal.instance.tsdb.enable=true
#canal.instance.tsdb.url=jdbc:mysql://127.0.0.1:3306/canal_tsdb
#canal.instance.tsdb.dbUsername=canal
#canal.instance.tsdb.dbPassword=canal

#canal.instance.standby.address =
#canal.instance.standby.journal.name =
#canal.instance.standby.position =
#canal.instance.standby.timestamp =
#canal.instance.standby.gtid=

# username/password
canal.instance.dbUsername=root
canal.instance.dbPassword=aaaaaa
canal.instance.connectionCharset = UTF-8
canal.instance.defaultDatabaseName =
# enable druid Decrypt database password
canal.instance.enableDruid=false
#canal.instance.pwdPublicKey=MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBALK4BUxdDltRRE5/zXpVEVPUgunvscYFtEip3pmLlhrWpacX7y7GCMo2/JM6LeHmiiNdH1FWgGCpUfircSwlWKUCAwEAAQ==

# table regex
canal.instance.filter.regex=gmall1128\\..*
# table black regex
canal.instance.filter.black.regex=

# mq config
canal.mq.topic=example
canal.mq.partition=0
# hash partition config
#canal.mq.partitionsNum=3
#canal.mq.partitionHash=mytest.person:id,mytest.role:id
#################################################
 */