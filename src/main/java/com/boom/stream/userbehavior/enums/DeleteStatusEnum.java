package com.boom.stream.userbehavior.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;
import java.util.Optional;

/**
 * @author aaron
 * @version 1.0
 */
@AllArgsConstructor
@Getter
public enum DeleteStatusEnum {

    NOT_DELETED(0, "未删除"),
    DELETED(1, "已删除");

    private final Integer code;

    private final String msg;

    public static Optional<DeleteStatusEnum> of(Integer code) {
        return Arrays.stream(DeleteStatusEnum.values()).filter(item -> item.getCode().equals(code)).findFirst();
    }

}
