package com.cmcc.dmp.enums;

import java.util.Arrays;
import java.util.List;

/**
 * requirement  请求枚举类
 *
 * @author zhangsl
 * @version 1.0
 * @date 2019/12/6 18:16
 */
public enum FlowNodeEnum {
    TotalRequest(1, "请求量Kpi"),
    Valid(2, "有效请求"),
    Advertising(3, "广告请求");

    private int code;
    private String desc;

    private FlowNodeEnum(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public static List<Integer> getRequestMode(){
        List<Integer> status = Arrays.asList(
                TotalRequest.code,
                Valid.code,
                Advertising.code
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
