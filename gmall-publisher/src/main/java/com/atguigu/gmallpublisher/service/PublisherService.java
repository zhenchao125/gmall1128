package com.atguigu.gmallpublisher.service;

import java.util.Map;

/**
 * @Author lzc
 * @Date 2020/5/29 16:43
 */
public interface PublisherService {
    /**
     * 获取日活
     * @param date
     * @return
     */
    Long getDau(String date);


    Map<String, Long> getHourDau(String date);
}
