package com.atguigu.gmallpublisher.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * 一个饼图
 * @Author lzc
 * @Date 2020/6/6 14:08
 */
public class Stat {
    private String title;
    private List<Option> options = new ArrayList<>();

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void  addOption(Option option) {
        options.add(option);
    }
}
