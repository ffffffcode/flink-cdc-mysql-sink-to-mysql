package com.boom.stream.userbehavior.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/2/10 11:34
 */
@Getter
@AllArgsConstructor
public enum UserBehaviorEnum {

    ORDER(1, "下单"),
    PAY(2, "支付"),
    USE(3, "核销"),
    REFUND(4, "退款"),
    PARTICIPATE_IN_OVERLORD_MEAL(5, "参与霸王餐"),
    COLLECT_PRODUCT(6, "收藏商品"),
    UN_COLLECT_PRODUCT(7, "取消收藏商品"),
    COLLECT_STORE(8, "收藏门店"),
    UN_COLLECT_STORE(9, "取消收藏门店"),
    COMMENT(10, "评论");


    private final Integer type;
    private final String name;
}
