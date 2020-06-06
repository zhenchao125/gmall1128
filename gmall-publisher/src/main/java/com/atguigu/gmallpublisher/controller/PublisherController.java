package com.atguigu.gmallpublisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublisher.bean.Option;
import com.atguigu.gmallpublisher.bean.SaleInfo;
import com.atguigu.gmallpublisher.bean.Stat;
import com.atguigu.gmallpublisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/5/29 16:18
 */
@RestController
public class PublisherController {

    @Autowired
    PublisherService service;

    // http://localhost:8070/realtime-total?date=2020-05-29
    @GetMapping("/realtime-total")
    public String realTotal(@RequestParam("date") String date) {

        ArrayList<Map<String, String>> result = new ArrayList<>();

        Map<String, String> map1 = new HashMap<>();
        map1.put("id", "dau");
        map1.put("name", "新增日活");
        map1.put("value", service.getDau(date).toString());
        result.add(map1);

        Map<String, String> map2 = new HashMap<>();
        map2.put("id", "new_mid");
        map2.put("name", "新增设备");
        map2.put("value", "233");
        result.add(map2);

        Map<String, String> map3 = new HashMap<>();
        map3.put("id", "order_amount");
        map3.put("name", "新增交易额");
        map3.put("value", service.getTotalAmount(date).toString());
        result.add(map3);


        return JSON.toJSONString(result);
    }

    // http://localhost:8070/realtime-hour?id=dau&date=2020-02-11
    // http://localhost:8070/realtime-hour?id=order_amount&date=2020-02-14
    @GetMapping("/realtime-hour")
    public String realtimeHour(String id, String date) {
        if ("dau".equals(id)) {
            Map<String, Long> today = service.getHourDau(date);
            Map<String, Long> yesterday = service.getHourDau(getYesterday(date));

            /*
            {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
                "today":{"12":38,"13":1233,"17":123,"19":688 }}
             */
            HashMap<String, Map<String, Long>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);
            return JSON.toJSONString(result);
        } else if ("order_amount".equals(id)) {
            Map<String, Double> today = service.getHourAmount(date);
            Map<String, Double> yesterday = service.getHourAmount(getYesterday(date));
            /*
            {"yesterday":{"11":383,"12":123,"17":88,"19":200 },
                "today":{"12":38,"13":1233,"17":123,"19":688 }}

             */
            Map<String, Map<String, Double>> result = new HashMap<>();
            result.put("today", today);
            result.put("yesterday", yesterday);
            return JSON.toJSONString(result);
        }


        return "ok";
    }

    /*
    根据指定的日期, 计算昨天
     */
    private String getYesterday(String date) {
//        return LocalDate.parse(date).plusDays(-1).toString();
        return LocalDate.parse(date).minusDays(1).toString();
    }

    //http://localhost:8070/sale_detail?date=2020-06-06&&startpage=1&&size=5&&keyword=手机小米
    @GetMapping("/sale_detail")
    public String saleDetail(String date, int startpage, int size, String keyword) throws IOException {
        Map<String, Object> genderResult = service.getSaleDetailAndAgg(date,
                keyword,
                startpage,
                size,
                "user_gender",
                2);

        Map<String, Object> ageResult = service.getSaleDetailAndAgg(date,
                keyword,
                startpage,
                size,
                "user_age",
                100);

        SaleInfo saleInfo = new SaleInfo();
        // 向最终结果封装数据
        // 1. 封装总数
        Integer total = (Integer) genderResult.get("total");
        saleInfo.setTotal(total);
        // 2. 封装明细(detail)
        List<HashMap> detail = (List<HashMap>)genderResult.get("detail");
        saleInfo.setDetail(detail);
        // 3. 封装饼图
        // 3.1 性别的饼图
        Stat genderStat = new Stat();
        genderStat.setTitle("用户性别占比");
        Map<String, Long> aggGender = (Map<String, Long>)genderResult.get("agg");  // M->25, F->25
        for (String key : aggGender.keySet()) {
            Option opt = new Option();
            opt.setName(key.equals("M") ? "男": "女");  // M   F
            opt.setValue(aggGender.get(key));  // 25  25
            // 把这个选项插入到饼图中
            genderStat.addOption(opt);
        }
        saleInfo.addStat(genderStat);  // 性别的饼图插入到最外层的对象中
        // 3.2 年龄饼图
        Stat ageStat = new Stat();
        ageStat.addOption(new Option("20以下", 0L));
        ageStat.addOption(new Option("20以上30以下", 0L));
        ageStat.addOption(new Option("30以上", 0L));
        Map<String, Long> aggAge = (Map<String, Long>)ageResult.get("agg");
        // "10"->20  "20"->15
        for (Map.Entry<String, Long> entry : aggAge.entrySet()) {
            int age = Integer.parseInt(entry.getKey()); // 把年龄转成int, 才可以比较大小
            Long value = entry.getValue();
            if(age < 20){
                Option opt = ageStat.getOptions().get(0);
                Long newValue = opt.getValue() + value;
                opt.setValue(newValue);
            }else if(age < 30){
                Option opt = ageStat.getOptions().get(1);
                Long newValue = opt.getValue() + value;
                opt.setValue(newValue);
            }else{
                Option opt = ageStat.getOptions().get(2);
                Long newValue = opt.getValue() + value;
                opt.setValue(newValue);
            }
        }
        saleInfo.addStat(ageStat);

        return JSON.toJSONString(saleInfo);
        // Gson  jackson
    }


}
/*
日活总数:
    [{"id":"dau","name":"新增日活","value":1200},
    {"id":"new_mid","name":"新增设备","value":233} ]

日活小时明细:

{"yesterday":{"11":383,"12":123,"17":88,"19":200 },
"today":{"12":38,"13":1233,"17":123,"19":688 }}

 */
