package com.boom.stream.demo;

import com.alibaba.fastjson.JSONObject;
import com.boom.stream.demo.entity.UserBehavior;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.*;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/27 10:41
 */
public class MySqlBinlogSourceMapAndSinkExample {

    public static void main(String[] args) throws Exception {
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("decimal.handling.mode", "string");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.0.10.13")
                .port(23100)
                .databaseList("mall_order") // set captured database
                .tableList("mall_order.bm_order") // set captured table
                .username("root")
                .password("a123456")
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000L);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL bm_order Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .map(new Mapper())
                .addSink(new MysqlSink())
                .name("MySQL user_behavior_flink_cdc Sink")
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog (Test Sink)");
    }

    private static class Mapper implements MapFunction<String, UserBehavior> {
        @Override
        public UserBehavior map(String binlogString) {
            JSONObject binlog = JSONObject.parseObject(binlogString);
            JSONObject source = binlog.getJSONObject("source");
            JSONObject after = binlog.getJSONObject("after");
            if ("c".equals(binlog.getString("op")) || "r".equals(binlog.getString("op"))) {
                UserBehavior userBehavior = new UserBehavior();
                userBehavior.setTenantId(after.getInteger("tenant_id"));
                userBehavior.setAreaId(after.getInteger("area_id"));
                userBehavior.setMemberId(after.getLong("member_id"));
                userBehavior.setEventTime(Instant.ofEpochMilli(source.getLong("ts_ms")));
                userBehavior.setBehaviorType(1);
                return userBehavior;
            }
            // TODO logic delete
            if ("u".equals(binlog.getString("op"))) {
                UserBehavior userBehavior = new UserBehavior();
                userBehavior.setTenantId(after.getInteger("tenant_id"));
                userBehavior.setAreaId(after.getInteger("area_id"));
                userBehavior.setMemberId(after.getLong("member_id"));
                userBehavior.setEventTime(Instant.ofEpochMilli(source.getLong("ts_ms")));
                userBehavior.setBehaviorType(2);
                return userBehavior;
            }
            if ("d".equals(binlog.getString("op"))) {
                JSONObject before = binlog.getJSONObject("before");
                UserBehavior userBehavior = new UserBehavior();
                userBehavior.setTenantId(before.getInteger("tenant_id"));
                userBehavior.setAreaId(before.getInteger("area_id"));
                userBehavior.setMemberId(before.getLong("member_id"));
                userBehavior.setEventTime(Instant.ofEpochMilli(source.getLong("ts_ms")));
                userBehavior.setBehaviorType(3);
                return userBehavior;
            }
            throw new RuntimeException("Flink CDC Mapping Exception, binlog: " + binlogString);
        }
    }


    private static class MysqlSink extends RichSinkFunction<UserBehavior> {

        private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        private PreparedStatement preparedStatement;
        private Connection connection;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = getConnection();
            String sql = "INSERT INTO mall_statistics.user_behavior_flink_cdc (tenant_id, area_id, member_id, event_time, behavior_type) VALUES (?, ?, ?, ?, ?)";
            preparedStatement = connection.prepareStatement(sql);
        }

        @Override
        public void close() throws Exception {
            super.close();
            // 关闭连接和释放资源
            if (connection != null) {
                connection.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

        @Override
        public void invoke(UserBehavior value, Context context) throws Exception {
            preparedStatement.setInt(1, value.getTenantId());
            preparedStatement.setInt(2, value.getAreaId());
            preparedStatement.setLong(3, value.getMemberId());
            preparedStatement.setString(4, value.getEventTime().atZone(ZoneId.of("+8")).format(DATE_TIME_FORMATTER));
            preparedStatement.setInt(5, value.getBehaviorType());
            preparedStatement.executeUpdate();
        }

        private static Connection getConnection() throws ClassNotFoundException, SQLException {
            Class.forName("com.mysql.cj.jdbc.Driver");
            return DriverManager.getConnection("jdbc:mysql://10.0.10.13:23100/mall_statistics?useUnicode=true&characterEncoding=UTF-8&useSSL=false", "root", "a123456");
        }

    }
}
