package com.boom.stream.userbehavior.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum OrderRefundVerifyStatusEnum {
    WAIT(0, "待审"),
    PASS(1, "通过"),
    REFUSE(2, "拒绝"),
    CANCEL(3, "取消");

    private final Integer code;

    private final String msg;

    public static OrderRefundVerifyStatusEnum getEnumByCode(Integer code) {
        OrderRefundVerifyStatusEnum[] items = values();
        for (OrderRefundVerifyStatusEnum item : items) {
            if (item.getCode().equals(code)) {
                return item;
            }
        }
        return null;
    }

    public static OrderRefundVerifyStatusEnum getEnumByMsg(String msg) {
        OrderRefundVerifyStatusEnum[] items = values();
        for (OrderRefundVerifyStatusEnum item : items) {
            if (item.getMsg().equals(msg)) {
                return item;
            }
        }
        return null;
    }
}
