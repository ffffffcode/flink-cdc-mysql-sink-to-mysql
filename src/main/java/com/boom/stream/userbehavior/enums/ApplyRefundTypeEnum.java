package com.boom.stream.userbehavior.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum ApplyRefundTypeEnum {

    MEMBER(0, "会员申请退款"),
    BACKSTAGE(1, "后台申请退款"),
    OVERDUE(2, "过期退款"),
    THIRD_PART_FAIL(3, "第三方业务处理失败自动退款"),
    OPENAPI(4, "openapi退款");

    private final Integer code;

    private final String msg;

    public static ApplyRefundTypeEnum getEnumByCode(Integer code) {
        ApplyRefundTypeEnum[] items = values();
        for (ApplyRefundTypeEnum item : items) {
            if (item.getCode().equals(code)) {
                return item;
            }
        }
        return null;
    }

    public static ApplyRefundTypeEnum getEnumByMsg(String msg) {
        ApplyRefundTypeEnum[] items = values();
        for (ApplyRefundTypeEnum item : items) {
            if (item.getMsg().equals(msg)) {
                return item;
            }
        }
        return null;
    }

}