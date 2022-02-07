package com.boom.stream.order;

import com.alibaba.fastjson.JSONObject;
import com.boom.stream.order.entity.BmOrder;
import com.boom.stream.order.entity.UserBehavior;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Instant;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/25 14:33
 */
public class BmOrderCdc {

    public static void main(String[] args) throws Exception {

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.enableCheckpointing(3000L);

//        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
//
//        String userBehaviorTableCreateSql = "CREATE TABLE user_behavior (\n" +
//                "    id BIGINT,\n" +
//                "    tenant_id INT,\n" +
//                "    area_id INT,\n" +
//                "    member_id BIGINT,\n" +
//                "    event_time TIMESTAMP,\n" +
//                "    behavior_type TINYINT,\n" +
//                "    PRIMARY KEY (id) NOT ENFORCED\n" +
//                "  ) WITH (\n" +
//                "    'connector' = 'jdbc',\n" +
//                "    'url' = 'jdbc:mysql://10.0.10.13:23100/mall_statistics',\n" +
//                "    'username' = 'root',\n" +
//                "    'password' = 'a123456',\n" +
//                "    'table-name' = 'user_behavior',\n" +
//                "    'sink.buffer-flush.max-rows'='1'\n" +
//                "  );";
//        tableEnvironment.executeSql(userBehaviorTableCreateSql);

        /*MySqlSource<DataChangeEvents> mySqlSource = MySqlSource.<DataChangeEvents>builder()
                .hostname("10.0.10.13")
                .port(23100)
                .databaseList("mall_order") // set captured database
                .tableList("mall_order.bm_order") // set captured table
                .username("root")
                .password("a123456")
                .deserializer(new BmOrderDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();*/


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.0.10.13")
                .port(23100)
                .databaseList("mall_order") // set captured database
                .tableList("mall_order.bm_order") // set captured table
                .username("root")
                .password("a123456")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Table 'bm_order' Source");

        dataStreamSource.print().setParallelism(1);

//        DataStreamSource<DataChangeEvents> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
//        dataStreamSource.setParallelism(1);
//        dataStreamSource.process()
//        SingleOutputStreamOperator<UserBehavior> userBehavior = dataStreamSource.map(new Mapper());
//        userBehavior.process()

//tableEnvironment.executeSql()

//        dataStreamSource.addSink(new UserBehaviorSink(tableEnvironment));

        env.execute("Print bm_order Snapshot + Binlog");

    }

    private static class UserBehaviorMapper implements MapFunction<String, String> {
        @Override
        public String map(String in) throws Exception {
            JSONObject jsonObject = JSONObject.parseObject(in);
           /* ObjectMapper objectMapper = new ObjectMapper();
            DataChangeEvents dataChangeEvents = objectMapper.readValue(in, DataChangeEvents.class);
            if ("c".equals(dataChangeEvents.getOp())) {
                System.out.println("新增订单");
            }*/
            if ("c".equals(jsonObject.getString("op"))) {
                throw new RuntimeException("新增订单");
            }
            return in;
        }
    }

    private static class UserBehaviorSink implements org.apache.flink.streaming.api.functions.sink.SinkFunction<String> {

        private final StreamTableEnvironment streamTableEnvironment;

        public UserBehaviorSink(StreamTableEnvironment streamTableEnvironment) {
            this.streamTableEnvironment = streamTableEnvironment;
        }


    }

    private static class Mapper implements MapFunction<DataChangeEvents, UserBehavior> {
        @Override
        public UserBehavior map(DataChangeEvents dataChangeEvents) throws Exception {
            UserBehavior userBehavior = new UserBehavior();
            userBehavior.setEventTime(Instant.ofEpochMilli(dataChangeEvents.getTsMs()));

            if ("c".equals(dataChangeEvents.getOp())) {
                userBehavior.setBehaviorType(1);
            }
            if ("u".equals(dataChangeEvents.getOp())) {
                userBehavior.setBehaviorType(2);
            }
            BmOrder after = (BmOrder) dataChangeEvents.getAfter();
            userBehavior.setMemberId(after.getMemberId());

            return userBehavior;
        }
    }
}
