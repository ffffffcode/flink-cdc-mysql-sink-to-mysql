package com.boom.stream.usergroup.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;


@Getter
@AllArgsConstructor
public enum UserGroupTargetEnum {

    ORDERS(0, "orders_count", "下单行为"),
    PAY(1, "pay_count", "支付行为"),
    CHECKED(2, "checked_count", "核销行为"),
    REFUND(3, "refund_count", "退款行为"),
    VISIT(4, "visit_count", "访问行为"),
    SHARE(5, "share_count", "分享行为"),
    PARTICIPATE_IN_ACTIVITIES(6, "participate_in_activities_count", "参加营销活动行为"),
    COLLECT(7, "collect_count", "收藏行为"),
    REVIEW(8, "review_count", "评论行为");

    private final Integer code;
    private final String val;
    private final String name;

    public static UserGroupTargetEnum get(Integer code) {
        for (UserGroupTargetEnum item : values()) {
            if (item.code.equals(code)) {
                return item;
            }
        }
        throw new IllegalArgumentException("用户分组行为参数错误");
    }

}
