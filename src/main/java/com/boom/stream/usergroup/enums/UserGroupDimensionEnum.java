package com.boom.stream.usergroup.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum UserGroupDimensionEnum {
    MONEY(1, "money", "金额"),
    COUNT(2, "count", "次数"),
    SINGLE_MAX_MONEY(3, "single_max_money", "最大金额");

    private final Integer code;
    private final String val;
    private final String name;

    public static UserGroupDimensionEnum get(Integer code) {
        for (UserGroupDimensionEnum item : values()) {
            if (item.code.equals(code)) {
                return item;
            }
        }
        throw new IllegalArgumentException("用户分组维度参数错误");
    }
}
