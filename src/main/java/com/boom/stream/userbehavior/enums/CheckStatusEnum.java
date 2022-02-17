package com.boom.stream.userbehavior.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum CheckStatusEnum {
    CANNOT_USE(0, "不可核销"),
    UNUSED(1, "未核销"),
    USED(2, "已核销"),
    OVER_TIME(3, "已过期"),
    ;

    private final Integer code;
    private final String msg;

    public static CheckStatusEnum getEnumByCode(Integer code) {
        CheckStatusEnum[] items = values();
        for (CheckStatusEnum item : items) {
            if (item.getCode().equals(code)) {
                return item;
            }
        }
        return null;
    }

    public static CheckStatusEnum getEnumByMsg(String msg) {
        CheckStatusEnum[] items = values();
        for (CheckStatusEnum item : items) {
            if (item.getMsg().equals(msg)) {
                return item;
            }
        }
        return null;
    }

}
