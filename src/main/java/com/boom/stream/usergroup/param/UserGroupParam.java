package com.boom.stream.usergroup.param;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/2/17 20:39
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserGroupParam {

    private Integer target;
    /**
     * 存在表达式
     */
    private Boolean isExist;
    /**
     * 值
     */
    private Integer value;

    private List<UserGroupSubParam> subParamList;

}
