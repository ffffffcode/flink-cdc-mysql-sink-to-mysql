package com.boom.stream.demo.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author aaron
 * @version 1.0
 */
@AllArgsConstructor
@Getter
public enum OrderStatusEnum {

    WAIT_PAY(0, "待支付"),
    WAIT_CHECK(1, "待核销"),
    ALREADY_CHECK(2, "已核销"),
    EXPIRED(3, "已过期"),
    CLOSED(4, "已关闭");

    private Integer code;

    private String msg;

    public static OrderStatusEnum getEnumByCode(Integer code) {
        OrderStatusEnum[] items = values();
        for (OrderStatusEnum item : items) {
            if (item.getCode().equals(code)) {
                return item;
            }
        }
        return null;
    }

    public static OrderStatusEnum getEnumByMsg(String msg) {
        OrderStatusEnum[] items = values();
        for (OrderStatusEnum item : items) {
            if (item.getMsg().equals(msg)) {
                return item;
            }
        }
        return null;
    }

}
