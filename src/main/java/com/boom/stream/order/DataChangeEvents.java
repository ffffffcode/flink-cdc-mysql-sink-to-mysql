package com.boom.stream.order;

import lombok.Data;
import lombok.ToString;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/26 14:54
 */
@Data
@ToString
public class DataChangeEvents<T> {

    /**
     * u=update,c=create(insert),d=delete
     */
    private String op;
    /**
     * 修改之前的实体数据，delete只有before
     */
    private T before;
    /**
     * 修改之后的实体数据，insert只有after
     */
    private T after;
    /**
     * 数据源信息
     */
    private String source;
    /**
     * 修改时间戳
     */
    private Long tsMs;

}
