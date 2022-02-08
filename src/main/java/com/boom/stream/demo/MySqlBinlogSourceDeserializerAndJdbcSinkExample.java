package com.boom.stream.demo;

import com.alibaba.fastjson.JSONObject;
import com.boom.stream.order.entity.UserBehavior;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Properties;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/27 10:41
 */
public class MySqlBinlogSourceDeserializerAndJdbcSinkExample {

    public static void main(String[] args) throws Exception {
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("decimal.handling.mode", "string");
        MySqlSource<UserBehavior> mySqlSource = MySqlSource.<UserBehavior>builder()
                .hostname("10.0.10.13")
                .port(23100)
                .databaseList("mall_order") // set captured database
                .tableList("mall_order.bm_order") // set captured table
                .username("root")
                .password("a123456")
                .debeziumProperties(debeziumProperties)
                .deserializer(new UserBehaviorDebeziumDeserializer()) // converts SourceRecord to UserBehavior
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(3000L);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL bm_order Source")
                // set 2 parallel source tasks
                .setParallelism(4)
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
                .setParallelism(2);

        env.execute("Sink UserBehavior");
    }

    private static class UserBehaviorDebeziumDeserializer implements DebeziumDeserializationSchema<UserBehavior> {

        private transient JsonConverter jsonConverter;

        @Override
        public void deserialize(SourceRecord record, Collector<UserBehavior> out) {
            if (jsonConverter == null) {
                // initialize jsonConverter
                jsonConverter = new JsonConverter();
                final HashMap<String, Object> configs = new HashMap<>(2);
                configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
                configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
                jsonConverter.configure(configs);
            }
            byte[] bytes =
                    jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());

            String binlogString = new String(bytes);
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
                out.collect(userBehavior);
                return;
            }
            // TODO logic delete
            if ("u".equals(binlog.getString("op"))) {
                UserBehavior userBehavior = new UserBehavior();
                userBehavior.setTenantId(after.getInteger("tenant_id"));
                userBehavior.setAreaId(after.getInteger("area_id"));
                userBehavior.setMemberId(after.getLong("member_id"));
                userBehavior.setEventTime(Instant.ofEpochMilli(source.getLong("ts_ms")));
                userBehavior.setBehaviorType(2);
                out.collect(userBehavior);
                return;
            }
            if ("d".equals(binlog.getString("op"))) {
                JSONObject before = binlog.getJSONObject("before");
                UserBehavior userBehavior = new UserBehavior();
                userBehavior.setTenantId(before.getInteger("tenant_id"));
                userBehavior.setAreaId(before.getInteger("area_id"));
                userBehavior.setMemberId(before.getLong("member_id"));
                userBehavior.setEventTime(Instant.ofEpochMilli(source.getLong("ts_ms")));
                userBehavior.setBehaviorType(3);
                out.collect(userBehavior);
                return;
            }
        }

        @Override
        public TypeInformation<UserBehavior> getProducedType() {
            return BasicTypeInfo.of(UserBehavior.class);
        }
    }
}
