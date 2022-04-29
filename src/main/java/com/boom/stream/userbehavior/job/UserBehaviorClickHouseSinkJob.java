package com.boom.stream.userbehavior.job;

import com.boom.stream.userbehavior.deserializer.UserBehaviorDebeziumDeserializer;
import com.boom.stream.userbehavior.deserializer.UserReviewBehaviorDebeziumDeserializer;
import com.boom.stream.userbehavior.entity.UserBehavior;
import com.ververica.cdc.connectors.mongodb.MongoDBSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/27 10:41
 */
@Slf4j
public class UserBehaviorClickHouseSinkJob {

    public static void main(String[] args) {
        try {
            Properties debeziumProperties = new Properties();
            // todo incremental.snapshot.chunk.size
            // snapshot.fetch.size
            // min.row.count.to.stream.results
//        debeziumProperties.put("debezium.snapshot.locking.mode", "none");
            debeziumProperties.setProperty("decimal.handling.mode", "string");
            MySqlSource<UserBehavior> mySqlSource = MySqlSource.<UserBehavior>builder()
                    .hostname("10.0.10.13")
                    .port(23100)
                    .databaseList("mall_order", "member", "mall_merchant")
                    .tableList("mall_order.order", "mall_order.order_check_code", "mall_order.order_refund", "member.my_collect", "mall_merchant.overlord_meal_participate_record")
                    .username("root")
                    .password("a123456")
                    .debeziumProperties(debeziumProperties)
                    .deserializer(new UserBehaviorDebeziumDeserializer())
                    .build();
            SourceFunction<UserBehavior> mongoSource = MongoDBSource.<UserBehavior>builder()
                    .hosts("10.0.10.13:23301,10.0.10.13:23302,10.0.10.13:23303")
                    .username("bm")
                    .password("a123456")
                    .database("bm-mall")
                    .collection("review")
                    .connectionOptions("authSource=bm-mall")
                    .deserializer(new UserReviewBehaviorDebeziumDeserializer())
                    .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            env.enableCheckpointing(3000L);

            DataStreamSource<UserBehavior> reviewMongodbOplogSource = env.addSource(mongoSource, "review MongoDB Oplog Source").setParallelism(1);

            env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "order/order_check_code/order_refund/my_collect/overlord_meal MySQL Binlog Source").setParallelism(1)
                    .connect(reviewMongodbOplogSource).map(new NothingToDoCoMapper()).name("merge")
                    .addSink(JdbcSink.sink(
                            "INSERT INTO statistics_user_behavior (tenant_id, area_id, member_id, event_time, behavior_type, behavior_name, source_id, actual_pay_money) VALUES (?, ?, ?, ?, ?, ?, ?, ?);",
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
                                    .withUrl("jdbc:clickhouse://10.0.10.13:25202/dwd?useUnicode=true&characterEncoding=UTF-8&useSSL=false&use_time_zone=UTC+8&use_server_time_zone=UTC+8")
                                    .withUsername("default")
                                    .withPassword("")
                                    .withDriverName("cc.blynk.clickhouse.ClickHouseDriver")
                                    .build()))
                    .name("statistics_user_behavior ClickHouse Sink")
                    .setParallelism(1);

            env.execute("UserBehavior Job(Order, Use, Refund, Collect, OverlordMeal, Review)");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class NothingToDoCoMapper implements CoMapFunction<UserBehavior, UserBehavior, UserBehavior> {
        @Override
        public UserBehavior map1(UserBehavior value) throws Exception {
            return value;
        }

        @Override
        public UserBehavior map2(UserBehavior value) throws Exception {
            return value;
        }
    }
}
