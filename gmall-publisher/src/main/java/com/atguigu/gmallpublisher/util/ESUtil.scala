package com.atguigu.gmallpublisher.util

import io.searchbox.client.JestClientFactory
import io.searchbox.client.config.HttpClientConfig


/**
 * Author atguigu
 * Date 2020/6/3 13:58
 */
object ESUtil {
    
    val factory = new JestClientFactory
    // 1.1.1 给工厂设置es的相关参数
    val esUrl = "http://hadoop102:8300" //注意换成自己的端口(9200)
    val config = new HttpClientConfig.Builder(esUrl)
        .maxTotalConnection(100) // 允许的最多客户端的个数
        .connTimeout(10000) // 连接es的超时时间
        .readTimeout(10000) // 读取数据的超时时间
        .multiThreaded(true)
        .build()
    factory.setHttpClientConfig(config)
    
    def getClient = factory.getObject
    
    
    def getDSL(date: String,
               keyword: String,
               startPage: Int,
               sizePerPage: Int,
               aggField: String,
               aggCount: Int) = {
        s"""
          |{
          |  "query": {
          |    "bool": {
          |      "filter": {
          |        "term": {
          |          "dt": "$date"
          |        }
          |      },
          |      "must": [
          |        {"match": {
          |          "sku_name": {
          |            "query": "$keyword",
          |            "operator": "and"
          |          }
          |        }}
          |      ]
          |    }
          |  },
          |  "aggs": {
          |    "group_by_$aggField": {
          |      "terms": {
          |        "field": "$aggField",
          |        "size": $aggCount
          |      }
          |    }
          |  },
          |  "from": ${(startPage - 1) * sizePerPage},
          |  "size": $sizePerPage
          |}
          |
          |""".stripMargin
    }
}

