package com.boom.stream.demo;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/27 10:41
 */
public class MySqlBinlogSourceMapExample {

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

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(4)
                .map(new Mapper())
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("Print MySQL Snapshot + Binlog (Test Map)");
    }

    private static class Mapper implements MapFunction<String, String> {
        @Override
        public String map(String s) throws Exception {
            JSONObject jsonObject = JSONObject.parseObject(s);
            if ("c".equals(jsonObject.getString("op"))) {
                return "create row id: " + jsonObject.getJSONObject("after").getString("id");
            }
            if ("u".equals(jsonObject.getString("op"))) {
                return "update row id: " + jsonObject.getJSONObject("after").getString("id");
            }
            if ("d".equals(jsonObject.getString("op"))) {
                return "delete row id: " + jsonObject.getJSONObject("before").getString("id");
            }
            return s;
        }
    }
}
