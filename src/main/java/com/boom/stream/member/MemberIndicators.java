package com.boom.stream.member;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/24 19:51
 */
public class MemberIndicators {

    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("10.0.10.13")
                .port(23100)
                .databaseList("mall_order", "member") // set captured database
                .tableList("mall_order.bm_order", "member.member_indicators") // set captured table
                .username("root")
                .password("a123456")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000L);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),"MySQL Source").setParallelism(4).print().setParallelism(1);

        env.execute("Print MySQL Snapshot + Binlog");

    }
}
