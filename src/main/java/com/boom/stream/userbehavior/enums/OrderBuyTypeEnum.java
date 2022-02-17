package com.boom.stream.userbehavior.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;


@AllArgsConstructor
@Getter
public enum OrderBuyTypeEnum {

    GENERAL(0, "普通"),
    OVERLORD_MEAL(1, "霸王餐"),
    OPENAPI(2, "openapi"),
    LIMITED_TIME_SALE(3,"限时特卖"),
    HOTEL_TRAVEL(4,"酒旅商品"),
    ;


    private final Integer code;

    private final String msg;

    public static OrderBuyTypeEnum getEnumByCode(Integer code) {
        OrderBuyTypeEnum[] items = values();
        for (OrderBuyTypeEnum item : items) {
            if (item.getCode().equals(code)) {
                return item;
            }
        }
        return null;
    }

    public static OrderBuyTypeEnum getEnumByMsg(String msg) {
        OrderBuyTypeEnum[] items = values();
        for (OrderBuyTypeEnum item : items) {
            if (item.getMsg().equals(msg)) {
                return item;
            }
        }
        return null;
    }

}
