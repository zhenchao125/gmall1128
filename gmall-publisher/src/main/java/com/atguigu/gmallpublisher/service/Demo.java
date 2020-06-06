package com.atguigu.gmallpublisher.service;

import com.atguigu.gmallpublisher.bean.Option;

import java.util.ArrayList;

/**
 * @Author lzc
 * @Date 2020/6/6 14:38
 */
public class Demo {
    public static void main(String[] args) {
        ArrayList<Option> options = new ArrayList<>();

        Option opt = new Option();

        opt.setName("a");
        opt.setValue(10L);
        options.add(opt);

        opt.setName("b");
        opt.setValue(20L);
        options.add(opt);

        System.out.println(options);
    }
}
