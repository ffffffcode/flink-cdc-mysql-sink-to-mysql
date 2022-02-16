package com.boom.stream.userbehavior.entity;

import lombok.Data;

import java.time.Instant;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/26 16:46
 */
@Data
public class UserBehavior {

    private String sourceId;

    private Long memberId;
    private Instant eventTime;
    private Integer behaviorType;
    private String behaviorName;

    private Integer tenantId;
    private Integer areaId;
}
