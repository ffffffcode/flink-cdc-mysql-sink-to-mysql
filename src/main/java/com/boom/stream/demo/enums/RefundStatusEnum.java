package com.boom.stream.demo.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum RefundStatusEnum {

    NOT_ALL_REFUNDED(0, "未全部退款"),
    ALL_APPLY_REFUND(1, "已全部申请退款"),
    ALL_REFUNDED(2, "已全部退款");

    private Integer code;

    private String msg;

    public static RefundStatusEnum getEnumByCode(Integer code) {
        RefundStatusEnum[] items = values();
        for (RefundStatusEnum item : items) {
            if (item.getCode().equals(code)) {
                return item;
            }
        }
        return null;
    }

    public static RefundStatusEnum getEnumByMsg(String msg) {
        RefundStatusEnum[] items = values();
        for (RefundStatusEnum item : items) {
            if (item.getMsg().equals(msg)) {
                return item;
            }
        }
        return null;
    }

}
