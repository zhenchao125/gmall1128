package com.atguigu.gmallpublisher.mapper;

public interface OrderMapper {
    // 得到指定日期的销售总额
    Double getTotalAmount(String date);
}
