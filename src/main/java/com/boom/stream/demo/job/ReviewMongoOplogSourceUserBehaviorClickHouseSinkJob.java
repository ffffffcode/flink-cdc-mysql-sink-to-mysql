package com.boom.stream.demo.job;

import com.boom.stream.demo.deserializer.UserReviewBehaviorDebeziumDeserializer;
import com.boom.stream.demo.entity.UserBehavior;
import com.ververica.cdc.connectors.mongodb.MongoDBSource;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/2/15 17:10
 */
public class ReviewMongoOplogSourceUserBehaviorClickHouseSinkJob {

    public static void main(String[] args) throws Exception {
        SourceFunction<UserBehavior> sourceFunction = MongoDBSource.<UserBehavior>builder()
                .hosts("10.0.10.13:23301,10.0.10.13:23302,10.0.10.13:23303")
                .username("bm")
                .password("a123456")
                .database("bm-mall")
                .collection("review")
                .connectionOptions("authSource=bm-mall")
                .deserializer(new UserReviewBehaviorDebeziumDeserializer())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(sourceFunction, "review Oplog Source")
                .addSink(JdbcSink.sink(
                        "INSERT INTO statistics_user_behavior (tenant_id, area_id, member_id, event_time, behavior_type, behavior_name, source_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
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
                .name("statistics_user_behavior ClickHouse Sink(Review)")
                .setParallelism(1);

        env.execute("UserBehavior Job(Review)");
    }

}
