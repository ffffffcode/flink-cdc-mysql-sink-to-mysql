package com.boom.stream.demo.job;

import com.boom.stream.demo.deserializer.UserBehaviorDebeziumDeserializer;
import com.boom.stream.demo.entity.UserBehavior;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/27 10:41
 */
public class OrderMySqlBinlogSourceUserBehaviorClickHouseSinkJob {

    public static void main(String[] args) throws Exception {
        try {
            Properties debeziumProperties = new Properties();
//        debeziumProperties.put("debezium.snapshot.locking.mode", "none");
            debeziumProperties.setProperty("decimal.handling.mode", "string");
            MySqlSource<UserBehavior> mySqlSource = MySqlSource.<UserBehavior>builder()
                    .hostname("10.0.10.13")
                    .port(23100)
                    .databaseList("mall_order")
                    .tableList("mall_order.order")
                    .username("root")
                    .password("a123456")
                    .debeziumProperties(debeziumProperties)
                    .deserializer(new UserBehaviorDebeziumDeserializer())
                    .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            env.enableCheckpointing(3000L);

            env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "order MySQL Binlog Source")
                    .setParallelism(4)
                    .addSink(JdbcSink.sink(
                            "INSERT INTO analyse.user_behavior_flink_cdc (tenant_id, area_id, member_id, event_time, behavior_type, behavior_name, source_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (ps, t) -> {
                                ps.setInt(1, t.getTenantId());
                                ps.setInt(2, t.getAreaId());
                                ps.setLong(3, t.getMemberId());
                                ps.setString(4, t.getEventTime().atZone(ZoneId.of("+8")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                                ps.setInt(5, t.getBehaviorType());
                                ps.setString(6, t.getBehaviorName());
                                ps.setLong(7, t.getSourceId());
                            },
                            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                    .withUrl("jdbc:clickhouse://10.0.10.13:25202/analyse?useUnicode=true&characterEncoding=UTF-8&useSSL=false&use_time_zone=UTC+8&use_server_time_zone=UTC+8")
                                    .withUsername("default")
                                    .withPassword("")
                                    .withDriverName("cc.blynk.clickhouse.ClickHouseDriver")
                                    .build()))
                    .name("user_behavior_flink_cdc ClickHouse Sink")
                    .setParallelism(1);

            env.execute("Order Binlog To UserBehavior Job");
        }catch (Exception e) {
            throw e;
        }
    }

}
