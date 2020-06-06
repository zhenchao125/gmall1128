package com.atguigu.gmallpublisher.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Author lzc
 * @Date 2020/6/6 14:07
 */
public class SaleInfo {
    private Integer total;
    //对象在这里new好, 只需要给外界提供add方法
    private List<Stat> stats = new ArrayList<>();
    // 详情
    private List<HashMap> detail;

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public List<Stat> getStats() {
        return stats;
    }

    public List<HashMap> getDetail() {
        return detail;
    }

    public void addStat(Stat stat) {
        stats.add(stat);
    }

    public void setDetail(List<HashMap> detail) {
        this.detail = detail;
    }
}
