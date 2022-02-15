package com.boom.stream.demo.enums;

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
public enum CollectTypeEnum {

    PRODUCT(0,"商品"),
    STORE(1,"门店");

    private final Integer code;
    private final String msg;

    public static Optional<CollectTypeEnum> of(Integer code){
        return Arrays.stream(CollectTypeEnum.values()).filter(item -> item.getCode().equals(code)).findFirst();
    }

}
