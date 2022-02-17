package com.boom.stream.userbehavior.job;

import com.boom.stream.userbehavior.deserializer.UserBehaviorDebeziumDeserializer;
import com.boom.stream.userbehavior.entity.UserBehavior;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
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
public class MySqlBinlogSourceUserBehaviorClickHouseSinkJob {

    public static void main(String[] args) throws Exception {
        try {
            Properties debeziumProperties = new Properties();
//        debeziumProperties.put("debezium.snapshot.locking.mode", "none");
            debeziumProperties.setProperty("decimal.handling.mode", "string");
            MySqlSource<UserBehavior> mySqlSource = MySqlSource.<UserBehavior>builder()
                    .hostname("10.0.10.13")
                    .port(23100)
                    .databaseList("mall_order", "member", "mall_merchant")
                    // TODO order order_check_code 过滤OpenApi数据
                    .tableList("mall_order.order", "mall_order.order_check_code", "mall_order.order_refund", "member.my_collect", "mall_merchant.overlord_meal_participate_record")
                    .username("root")
                    .password("a123456")
                    .debeziumProperties(debeziumProperties)
                    .deserializer(new UserBehaviorDebeziumDeserializer())
                    .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            env.enableCheckpointing(3000L);

            env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "order/order_check_code/order_refund/my_collect/overlord_meal MySQL Binlog Source")
                    .setParallelism(1)
                    .addSink(JdbcSink.sink(
                            "INSERT INTO statistics_user_behavior (tenant_id, area_id, member_id, event_time, behavior_type, behavior_name, source_id, actual_pay_money) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (ps, t) -> {
                                ps.setInt(1, t.getTenantId());
                                Integer areaId = t.getAreaId();
                                if (areaId == null) {
                                    areaId = -1;
                                }
                                ps.setInt(2, areaId);
                                ps.setLong(3, t.getMemberId());
                                ps.setString(4, t.getEventTime().atZone(ZoneId.of("+8")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                                ps.setInt(5, t.getBehaviorType());
                                ps.setString(6, t.getBehaviorName());
                                ps.setString(7, t.getSourceId());
                                ps.setBigDecimal(8, t.getActualPayMoney());
                            },
                            JdbcExecutionOptions.builder()
                                    .withBatchSize(1000)
                                    .withBatchIntervalMs(200)
                                    .withMaxRetries(5)
                                    .build(),
                            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                    .withUrl("jdbc:clickhouse://10.0.10.13:25202/dim?useUnicode=true&characterEncoding=UTF-8&useSSL=false&use_time_zone=UTC+8&use_server_time_zone=UTC+8")
                                    .withUsername("default")
                                    .withPassword("")
                                    .withDriverName("cc.blynk.clickhouse.ClickHouseDriver")
                                    .build()))
                    .name("statistics_user_behavior ClickHouse Sink")
                    .setParallelism(1);

            env.execute("UserBehavior Job(Order, Use, Refund, Collect, OverlordMeal)");
        } catch (Exception e) {
            throw e;
        }
    }

}
