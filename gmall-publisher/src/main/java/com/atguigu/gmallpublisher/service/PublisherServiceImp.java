package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import com.atguigu.gmallpublisher.mapper.OrderMapper;
import com.atguigu.gmallpublisher.util.ESUtil;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/5/29 16:45
 */
@Service
public class PublisherServiceImp implements PublisherService {

    @Autowired
    DauMapper dau;

    @Override
    public Long getDau(String date) {
        return dau.getDau(date);
    }

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map<String, Object>> hourDauList = dau.getHourDau(date);
        /*
        logHour             count
        logHour->"14"         count->66
        "17"         181

        ----

        "14"->66  "17"->181   "18"->200
         */
        Map<String, Long> result = new HashMap<>();
        for (Map<String, Object> map : hourDauList) {
            String key = (String) map.get("LOGHOUR");
            Long value = (Long) map.get("COUNT");
            result.put(key, value);
        }
        return result;
    }

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Double getTotalAmount(String date) {
        // 当指定日期中没有数据的时候, 则这个地方会得到null
        Double totalAmount = orderMapper.getTotalAmount(date);
        return totalAmount == null ? 0 : totalAmount;
    }

    // 每个小时的销售额
    @Override
    public Map<String, Double> getHourAmount(String date) {
        // Map("create_hour"->"10", "sum" -> 1000)  => "10"->1000
        List<Map<String, Object>> hourAmountList = orderMapper.getHourAmount(date);
        Map<String, Double> result = new HashMap<>();
        for (Map<String, Object> map : hourAmountList) {
            String key = (String) map.get("CREATE_HOUR");
            Double value = ((BigDecimal) map.get("SUM")).doubleValue();
            result.put(key, value);
        }

        return result;
}

    @Override
    public Map<String, Object> getSaleDetailAndAgg(String date,
                                                   String keyword,
                                                   int startPage,
                                                   int sizePerPage,
                                                   String aggField,
                                                   int aggCount) throws IOException {
        // 0. 获取查询字符串
        String dsl =  ESUtil.getDSL(date, keyword, startPage, sizePerPage, aggField, aggCount);
        // 1. 获取es客户端
        JestClient client = ESUtil.getClient();
        Search search = new Search.Builder(dsl).build();
        // 2. 执行查询, 得到所有的数据
        SearchResult searchResult = client.execute(search);

        // 3. 从searchResult中得到我们想要的数据
        Map<String, Object> result = new HashMap<>();
        // 3.1 总数
        Integer total = searchResult.getTotal();
        result.put("total", total);
        // 3.2 获取详情
        ArrayList<HashMap> detail = new ArrayList<>();
        List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
        for (SearchResult.Hit<HashMap, Void> hit : hits) {
            HashMap source = hit.source;
            detail.add(source);
        }
        result.put("detail", detail);
        // 3.3 聚合结果  TODO



        return result;
    }
}
/*
"total"-> 100 ,
"agg"-> Map<..>
"detail": List<Map>
 */
