package com.atguigu.gmallpublisher.service;

import java.io.IOException;
import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/5/29 16:43
 */
public interface PublisherService {
    /**
     * 获取日活
     *
     * @param date
     * @return
     */
    Long getDau(String date);


    Map<String, Long> getHourDau(String date);


    // 获取总的销售额
    Double getTotalAmount(String date);

    // 获取每小时的销售额
    // Map("10"->1000.1, "11" -> 2000,...)
    Map<String, Double> getHourAmount(String date);


    // 完成从es的查数据的底层操作
    // "total"-> 100 , "agg"-> Map<..>   "detail": List<Map>
    Map<String, Object> getSaleDetailAndAgg(String date,
                                            String keyword,
                                            int startPage,  // 第几页数据
                                            int sizePerPage,  // 每页多少条数据
                                            String aggField, // 聚合字段
                                            int aggCount) throws IOException;  // 一共最多多个聚合结果


}
