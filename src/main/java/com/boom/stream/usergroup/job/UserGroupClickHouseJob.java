package com.boom.stream.usergroup.job;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.MybatisSqlSessionFactoryBuilder;
import com.boom.stream.usergroup.db.mapper.UserGroupMapper;
import com.boom.stream.usergroup.enums.UserGroupDimensionEnum;
import com.boom.stream.usergroup.enums.UserGroupOperatorEnum;
import com.boom.stream.usergroup.param.UserGroupParam;
import com.boom.stream.usergroup.param.UserGroupSubParam;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.logging.stdout.StdOutImpl;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/2/16 14:44
 */
@Slf4j
public class UserGroupClickHouseJob {

    private static final SqlSessionFactory SQL_SESSION_FACTORY = initSqlSessionFactory();
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static String redisKey = "flink:userGroup:620dbcb2d47a027c8bdcaa93";

    public static void main(String[] args) throws Exception {
        // TODO 是否支持修改，规则是否用消息传输
        // TODO 消费消息触发

        // TODO 解析规则

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkJedisPoolConfig flinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("10.0.10.13")
                .setPort(23200)
                .setDatabase(0)
                .setPassword("a123456")
                .build();

        /*FlinkJedisClusterConfig flinkJedisClusterConfig = new FlinkJedisClusterConfig.Builder()
                .setNodes(new HashSet<>(Collections.singletonList(new InetSocketAddress(5601)))).build();*/

        DataStreamSource<UserGroupParam> userGroupParamDataStreamSource = env.fromCollection(testDate()).setParallelism(1);
        // TODO 优化去重
        userGroupParamDataStreamSource.process(new Query()).setParallelism(1)
                .flatMap(new FlatLongMapper())

                // Redis去重 单节点
                .addSink(new RedisSink<>(flinkJedisPoolConfig, new UserGroupRedisMapper())).setParallelism(1).name("UserGroup Redis Sink");
                // Redis去重 集群
//                .addSink(new RedisSink<>(flinkJedisClusterConfig, new UserGroupRedisMapper())).setParallelism(1).name("UserGroup Redis Sink");

//                .print().setParallelism(1).name("Print MemberId");

        env.execute("Print");
    }

    private static List<UserGroupParam> testDate() {
        List<UserGroupParam> userGroupParamList = new ArrayList<>();
        // order
        userGroupParamList.add(UserGroupParam.builder().target(0).isExist(false).value(14).build());
        // pay
        userGroupParamList.add(UserGroupParam.builder().target(1).isExist(true).value(30).subParamList(
                new ArrayList<>(Arrays.asList(
                        UserGroupSubParam.builder().dimension(2).operator(2).value(1).build(),
                        UserGroupSubParam.builder().dimension(2).operator(3).value(2).build(),
                        UserGroupSubParam.builder().dimension(1).operator(3).value(100).build(),
                        UserGroupSubParam.builder().dimension(3).operator(3).value(50).build()
                ))).build());
        return userGroupParamList;
    }

    private static class Query extends ProcessFunction<UserGroupParam, List<Long>> {

//        Set<Long> result = new HashSet<>();

        @Override
        public void processElement(UserGroupParam userGroupParam, Context ctx, Collector<List<Long>> out) {
            try (SqlSession session = SQL_SESSION_FACTORY.openSession(true)) {
                UserGroupMapper mapper = session.getMapper(UserGroupMapper.class);
                String havingSql = parse(userGroupParam.getSubParamList());
                String time = LocalDate.now().minusDays(userGroupParam.getValue() - 1).atStartOfDay().format(DATE_TIME_FORMATTER);
                if (userGroupParam.getIsExist()) {
                    List<Long> memberIdList = mapper.exist(userGroupParam.getTarget(), time, havingSql);
//                    result.addAll(memberIdList);
                    out.collect(memberIdList);
                    return;
                }
                List<Long> memberIdList = mapper.notExist(userGroupParam.getTarget(), time, havingSql);
//                result.addAll(memberIdList);
                out.collect(memberIdList);
            }
        }

        private String parse(List<UserGroupSubParam> subParamList) {
            if (subParamList == null || subParamList.isEmpty()) {
                return null;
            }
            List<String> conditionList = new LinkedList<>();
            subParamList.stream().collect(Collectors.groupingBy(UserGroupSubParam::getDimension)).forEach((k, v) -> {
                for (UserGroupSubParam userGroupSubParam : v) {
                    StringBuilder stringBuilder = new StringBuilder();
                    UserGroupDimensionEnum userGroupDimensionEnum = UserGroupDimensionEnum.get(userGroupSubParam.getDimension());
                    switch (userGroupDimensionEnum) {
                        case MONEY:
                            stringBuilder.append("SUM(actual_pay_money)");
                            break;
                        case COUNT:
                            stringBuilder.append("COUNT(*)");
                            break;
                        case SINGLE_MAX_MONEY:
                            stringBuilder.append("MAX(actual_pay_money)");
                            break;
                        default:
                    }
                    UserGroupOperatorEnum userGroupOperatorEnum = UserGroupOperatorEnum.get(userGroupSubParam.getOperator());
                    switch (userGroupOperatorEnum) {
                        case LESS_THAN:
                            stringBuilder.append("<");
                            break;
                        case GREATER_THAN:
                            stringBuilder.append(">");
                            break;
                        case LESS_THAN_EQUALS:
                            stringBuilder.append("<=");
                            break;
                        case GREATER_THAN_EQUALS:
                            stringBuilder.append(">=");
                            break;
                        default:
                    }
                    stringBuilder.append(userGroupSubParam.getValue());
                    conditionList.add(stringBuilder.toString());
                }
            });

            return String.join(" AND ", conditionList);
        }
    }

    public static SqlSessionFactory initSqlSessionFactory() {
        Reader reader = null;
        try {
            reader = Resources.getResourceAsReader("mybatis-config.xml");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new MybatisSqlSessionFactoryBuilder().build(reader, "dev");
    }

    private static class UserGroupRedisMapper implements RedisMapper<String> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SADD);
        }

        @Override
        public String getKeyFromData(String value) {
            return redisKey;
        }

        @Override
        public String getValueFromData(String value) {
            return value;
        }

    }

    private static class FlatLongMapper implements FlatMapFunction<List<Long>, String> {
        @Override
        public void flatMap(List<Long> value, Collector<String> out) throws Exception {
            for (Long l : value) {
                out.collect(String.valueOf(l));
            }
        }
    }
}
