package com.boom.stream.usergroup.job;

import com.baomidou.mybatisplus.core.MybatisSqlSessionFactoryBuilder;
import com.boom.stream.usergroup.db.mapper.UserGroupMapper;
import com.boom.stream.usergroup.enums.UserGroupDimensionEnum;
import com.boom.stream.usergroup.enums.UserGroupOperatorEnum;
import com.boom.stream.usergroup.param.UserGroupParam;
import com.boom.stream.usergroup.param.UserGroupSubParam;
import com.google.common.collect.Lists;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;

import java.io.IOException;
import java.io.Reader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
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

        DataStreamSource<UserGroupParam> userGroupParamDataStreamSource = env.fromCollection(testDate()).setParallelism(1);
        userGroupParamDataStreamSource.process(new Query()).setParallelism(1)
                .process(new Split()).setParallelism(1)
                .addSink(new RedisSyncSink()).setParallelism(1).name("UserGroup Redis Sink");
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

        @Override
        public void processElement(UserGroupParam userGroupParam, Context ctx, Collector<List<Long>> out) {
            try (SqlSession session = SQL_SESSION_FACTORY.openSession(true)) {
                UserGroupMapper mapper = session.getMapper(UserGroupMapper.class);
                String havingSql = parse(userGroupParam.getSubParamList());
                String time = LocalDate.now().minusDays(userGroupParam.getValue() - 1).atStartOfDay().format(DATE_TIME_FORMATTER);
                if (userGroupParam.getIsExist()) {
                    List<Long> memberIdList = mapper.exist(userGroupParam.getTarget(), time, havingSql);
                    out.collect(memberIdList);
                    return;
                }
                List<Long> memberIdList = mapper.notExist(userGroupParam.getTarget(), time, havingSql);
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

    private static class Split extends ProcessFunction<List<Long>, List<Long>> {
        @Override
        public void processElement(List<Long> value, Context ctx, Collector<List<Long>> out) throws Exception {
            for (List<Long> list : Lists.partition(value, 10000)) {
                out.collect(new ArrayList<>(list));
            }
        }
    }


    private static class RedisSyncSink extends RichSinkFunction<List<Long>> {

        private RedisClient redisClient;
        private StatefulRedisConnection<String, String> connect;
        private RedisCommands<String, String> redisSyncCommands;

        @Override
        public void open(Configuration parameters) throws Exception {
            RedisURI redisUri = RedisURI.Builder.redis("10.0.10.13", 23200).withPassword("a123456").withDatabase(0).build();
            redisClient = RedisClient.create(redisUri);
            connect = redisClient.connect();
            redisSyncCommands = connect.sync();

            // 集群
            // RedisClusterClient redisClusterClient = RedisClusterClient.create("");
        }

        @Override
        public void invoke(List<Long> value, Context context) throws Exception {
            redisSyncCommands.sadd(redisKey, value.stream().map(String::valueOf).toArray(String[]::new));
        }

        @Override
        public void close() throws Exception {
            if (redisSyncCommands != null) {
                redisSyncCommands.shutdown(true);
            }
            if (connect != null) {
                connect.close();
            }
            if (redisClient != null) {
                redisClient.shutdown();
            }
        }
    }


    /*private static class RedisAsyncSink extends RichSinkFunction<List<Long>> {

        private RedisAsyncCommands<String, String> redisAsyncCommands;
        private StatefulRedisConnection<String, String> connect;

        @Override
        public void open(Configuration parameters) throws Exception {
            RedisURI redisUri = RedisURI.Builder.redis("10.0.10.13", 23200).withPassword("a123456").withDatabase(0).build();
            RedisClient redisClient = RedisClient.create(redisUri);
            connect = redisClient.connect();
            redisAsyncCommands = connect.async();
        }

        @Override
        public void invoke(List<Long> value, Context context) throws Exception {
            redisAsyncCommands.sadd(redisKey, value.stream().map(String::valueOf).toArray(String[]::new));
        }

        @Override
        public void close() throws Exception {
            if (redisAsyncCommands != null) {
                redisAsyncCommands.shutdown(true);
            }
            if (connect != null) {
                connect.close();
            }
        }
    }*/

    public static SqlSessionFactory initSqlSessionFactory() {
        Reader reader = null;
        try {
            reader = Resources.getResourceAsReader("mybatis-config.xml");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new MybatisSqlSessionFactoryBuilder().build(reader, "dev");
    }

}
