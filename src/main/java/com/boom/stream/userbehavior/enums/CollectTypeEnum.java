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
public enum CollectTypeEnum {

    PRODUCT(0,"εε"),
    STORE(1,"ι¨εΊ");

    private final Integer code;
    private final String msg;

    public static Optional<CollectTypeEnum> of(Integer code){
        return Arrays.stream(CollectTypeEnum.values()).filter(item -> item.getCode().equals(code)).findFirst();
    }

}
