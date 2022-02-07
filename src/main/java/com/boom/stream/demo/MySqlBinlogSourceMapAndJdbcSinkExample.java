package com.boom.stream.demo;

import com.alibaba.fastjson.JSONObject;
import com.boom.stream.order.entity.UserBehavior;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/27 10:41
 */
public class MySqlBinlogSourceMapAndJdbcSinkExample {

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
                .addSink(JdbcSink.sink(
                        "INSERT INTO mall_statistics.user_behavior_flink_cdc (tenant_id, area_id, member_id, event_time, behavior_type) VALUES (?, ?, ?, ?, ?)",
                        (ps, t) -> {
                            ps.setInt(1, t.getTenantId());
                            ps.setInt(2, t.getAreaId());
                            ps.setLong(3, t.getMemberId());
                            ps.setString(4, t.getEventTime().atZone(ZoneId.of("+8")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                            ps.setInt(5, t.getBehaviorType());
                        },
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://10.0.10.13:23100/mall_statistics?useUnicode=true&characterEncoding=UTF-8&useSSL=false")
                                .withUsername("root")
                                .withPassword("a123456")
                                .withDriverName("com.mysql.cj.jdbc.Driver")
                                .build()))
                .name("MySQL user_behavior_flink_cdc Sink")
                .setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog (Test JDBC Sink)");
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

}
