package com.boom.stream.usergroup.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum UserGroupOperatorEnum {

    GREATER_THAN(0, ">", "大于"),
    LESS_THAN(1, "<", "小于"),
    GREATER_THAN_EQUALS(2, ">=", "大于等于"),
    LESS_THAN_EQUALS(3, "<=", "小于等于");


    private final Integer code;
    private final String expression;
    private final String name;

    public static UserGroupOperatorEnum get(Integer code) {
        for (UserGroupOperatorEnum item : values()) {
            if (item.code.equals(code)) {
                return item;
            }
        }
        throw new IllegalArgumentException("用户分组维度操作符参数错误");
    }
}
