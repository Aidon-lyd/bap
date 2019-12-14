package com.cmcc.dmp.bean;

/**
 * requirement  TagsBean对象 ES
 *
 * @author zhangsl
 * @version 1.0
 * @date 2019/12/13 21:35
 */
public class TagsBean {
    private String rowkey;
    private String label;

    public TagsBean() {
    }

    @Override
    public String toString() {
        return "TagsBean{" +
                "rowkey='" + rowkey + '\'' +
                ", label='" + label + '\'' +
                '}';
    }

    public TagsBean(String rowkey, String label) {
        this.rowkey = rowkey;
        this.label = label;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getRowkey() {
        return rowkey;
    }

    public String getLabel() {
        return label;
    }
}
