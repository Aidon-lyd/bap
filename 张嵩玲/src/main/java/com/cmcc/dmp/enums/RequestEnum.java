package com.cmcc.dmp.enums;

import java.util.Arrays;
import java.util.List;

/**
 * requirement  动作枚举类
 *
 * @author zhangsl
 * @version 1.0
 * @date 2019/12/6 18:16
 */
public enum RequestEnum {
    Request(1, "请求"),
    Show(2, "展示"),
    Click(3, "点击");

    private int code;
    private String desc;

    private RequestEnum(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<Integer> getRequestMode(){
        List<Integer> status = Arrays.asList(
                Request.code,
                Show.code,
                Click.code
        );
        return status;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

}
