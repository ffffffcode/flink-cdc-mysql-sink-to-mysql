package com.boom.stream.order.entity;

import lombok.Data;

import java.time.Instant;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/26 16:46
 */
@Data
public class UserBehavior {

    private Long id;

    private Long memberId;
    private Instant eventTime;
    private Integer behaviorType;

    private Integer tenantId;
    private Integer areaId;
}
