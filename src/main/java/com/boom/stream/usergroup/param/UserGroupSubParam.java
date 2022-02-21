package com.boom.stream.usergroup.param;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/2/17 20:39
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserGroupSubParam {

    /**
     * 维度
     */
    private Integer dimension;

    /**
     * 运算符
     */
    private Integer operator;

    /**
     * 值
     */
    private Integer value;
}
