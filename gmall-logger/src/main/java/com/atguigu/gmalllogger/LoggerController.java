package com.atguigu.gmalllogger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.common.Constant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Author lzc
 * @Date 2020/5/27 10:20
 */
/*@Controller
@ResponseBody   // 把纯字符串给前端*/
@RestController   // @Controller + @ResponseBody
public class LoggerController {

    // 定义方法, 来处理http请求
    // http://localhost:8080/log?a=b
    //@GetMapping("/log")
    // http://localhost:8080/log     请求体: log=....
    @PostMapping("/log")
    public String doLogger(@RequestParam("log") String log) {
        // 1. 给日志加时间戳  ts
        log = addTS(log);
        // 2. 日志落盘. (这个个数据, 有可能会给离线需求使用)
        // 2.1. sprintboot默认使用logging, 需要去掉logging换成 log4j
        // 2.2. 配置log4j\
        saveToDisk(log);
        // 3. 实时的把数据写入到Kafka (给实时需求使用)
        sendToKafka(log);

        return "ok";
    }


    @Autowired
    private KafkaTemplate<String, String> kafka;

    /**
     * 把日志信息发送到kafka
     *
     * @param log
     */
    private void sendToKafka(String log) {
        // 1. 写一个生产者
        // 2. 不同的日志发送不到不同的topic
        if (log.contains("startup")) {
            kafka.send(Constant.STARTUP_TOPIC, log);
        } else {
            kafka.send(Constant.EVENT_TOPIC, log);
        }
    }

    private Logger logger = LoggerFactory.getLogger(LoggerController.class);

    /**
     * 把日志落盘
     *
     * @param log
     */
    private void saveToDisk(String log) {
        logger.info(log);
    }


    private String addTS(String log) {
        // 1. 先解析成json对象
        JSONObject jsonObj = JSON.parseObject(log);
        // 2. 添加一个时间戳kv
        jsonObj.put("ts", System.currentTimeMillis());
        // 3. 把json转成字符串返回
        return jsonObj.toJSONString();
    }

}
