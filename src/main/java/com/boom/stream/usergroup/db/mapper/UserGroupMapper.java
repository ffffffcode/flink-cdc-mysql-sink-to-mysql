package com.boom.stream.usergroup.db.mapper;

import com.boom.stream.usergroup.param.UserGroupParam;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/2/19 10:20
 */
public interface UserGroupMapper {

    List<Long> exist(@Param("target") Integer target, @Param("time") String time, @Param("havingSql") String havingSql);

    List<Long> notExist(@Param("target") Integer target, @Param("time") String time, @Param("havingSql") String havingSql);

}
