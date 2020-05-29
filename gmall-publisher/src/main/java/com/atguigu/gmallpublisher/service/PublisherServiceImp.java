package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.mapper.DauMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

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
}
