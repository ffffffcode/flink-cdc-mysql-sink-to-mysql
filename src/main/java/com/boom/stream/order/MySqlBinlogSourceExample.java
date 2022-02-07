package com.boom.stream.order;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.boom.stream.order.entity.BmOrder;
import com.boom.stream.order.entity.UserBehavior;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.Properties;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/27 10:41
 */
public class MySqlBinlogSourceExample {
    private static final String USER_BEHAVIOR_MYSQL_URL = "jdbc:mysql://10.0.10.13:23100/mall_statistics";

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
                .deserializer(new UserBehaviorDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // enable checkpoint
        env.enableCheckpointing(3000L);

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(1)
                .addSink(new MySqlSink()).name("UserBehaviorSink");
//                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute("BmOrder Sink To UserBehavior");
    }

    private static UserBehavior convertToUserBehavior(String s) {
        // {"before":null,"after":{"id":3,"tenant_id":1,"area_id":0,"overdue_time":"2021-06-05T01:36:32Z","buy_type":0,"transaction_type":3,"sharer_member_id":0,"member_id":1394220708019707906,"member_name":"李定辉","member_avatar_url":"https://file.cdn.tanchi.shop/dev/user/avatar/14a351adcb42edda7939d5123f916425","member_open_id":"oCOap5BHK-STBBILDcPx8ZATxE2M","member_mobile":"13267620116","member_notes":"","business_id":0,"business_name":"倍享话费充值","business_logo":"","product_id":0,"product_name":"测试产品1","product_pic":"","product_num":1,"product_price":"Cg==","total_money":"Cg==","pay_money":"Cg==","actual_pay_money":null,"discount_money":"AA==","order_status":4,"settle_status":0,"settle_time":null,"order_time":"2021-06-05T01:26:32Z","finish_time":null,"pay_time":null,"apply_refund_time":null,"actual_refund_time":null,"close_time":"2021-06-05T01:36:33Z","comment_status":0,"create_time":"2021-06-05T01:26:32Z","update_time":"2021-07-29T07:12:51Z","update_user_id":0,"bm_master_order_id":1400987366369402881},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1643252640000,"snapshot":"false","db":"mall_order","sequence":null,"table":"bm_order","server_id":1,"gtid":null,"file":"mysql-bin.000026","pos":4165168,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1643252640690,"transaction":null}
        // {"before":{"id":3,"tenant_id":1,"area_id":0,"overdue_time":"2021-06-05T01:36:32Z","buy_type":0,"transaction_type":3,"sharer_member_id":0,"member_id":1394220708019707906,"member_name":"李定辉","member_avatar_url":"https://file.cdn.tanchi.shop/dev/user/avatar/14a351adcb42edda7939d5123f916425","member_open_id":"oCOap5BHK-STBBILDcPx8ZATxE2M","member_mobile":"13267620116","member_notes":"","business_id":0,"business_name":"倍享话费充值","business_logo":"","product_id":0,"product_name":"测试产品1","product_pic":"","product_num":1,"product_price":"Cg==","total_money":"Cg==","pay_money":"Cg==","actual_pay_money":null,"discount_money":"AA==","order_status":4,"settle_status":0,"settle_time":null,"order_time":"2021-06-05T01:26:32Z","finish_time":null,"pay_time":null,"apply_refund_time":null,"actual_refund_time":null,"close_time":"2021-06-05T01:36:33Z","comment_status":0,"create_time":"2021-06-05T01:26:32Z","update_time":"2021-07-29T07:12:51Z","update_user_id":0,"bm_master_order_id":1400987366369402881},"after":{"id":3,"tenant_id":1,"area_id":0,"overdue_time":"2021-06-05T01:36:32Z","buy_type":0,"transaction_type":3,"sharer_member_id":0,"member_id":1394220708019707906,"member_name":"fffff","member_avatar_url":"https://file.cdn.tanchi.shop/dev/user/avatar/14a351adcb42edda7939d5123f916425","member_open_id":"oCOap5BHK-STBBILDcPx8ZATxE2M","member_mobile":"13267620116","member_notes":"","business_id":0,"business_name":"倍享话费充值","business_logo":"","product_id":0,"product_name":"测试产品1","product_pic":"","product_num":1,"product_price":"Cg==","total_money":"Cg==","pay_money":"Cg==","actual_pay_money":null,"discount_money":"AA==","order_status":4,"settle_status":0,"settle_time":null,"order_time":"2021-06-05T01:26:32Z","finish_time":null,"pay_time":null,"apply_refund_time":null,"actual_refund_time":null,"close_time":"2021-06-05T01:36:33Z","comment_status":0,"create_time":"2021-06-05T01:26:32Z","update_time":"2022-01-27T03:29:36Z","update_user_id":0,"bm_master_order_id":1400987366369402881},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1643254176000,"snapshot":"false","db":"mall_order","sequence":null,"table":"bm_order","server_id":1,"gtid":null,"file":"mysql-bin.000026","pos":4168693,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1643254176375,"transaction":null}
        // {"before":{"id":1,"tenant_id":1,"area_id":0,"overdue_time":"2021-06-05T01:36:32Z","buy_type":0,"transaction_type":3,"sharer_member_id":0,"member_id":1394220708019707906,"member_name":"李定辉","member_avatar_url":"https://file.cdn.tanchi.shop/dev/user/avatar/14a351adcb42edda7939d5123f916425","member_open_id":"oCOap5BHK-STBBILDcPx8ZATxE2M","member_mobile":"13267620116","member_notes":"","business_id":0,"business_name":"倍享话费充值","business_logo":"","product_id":0,"product_name":"测试产品1","product_pic":"","product_num":1,"product_price":"Cg==","total_money":"Cg==","pay_money":"Cg==","actual_pay_money":null,"discount_money":"AA==","order_status":4,"settle_status":0,"settle_time":null,"order_time":"2021-06-05T01:26:32Z","finish_time":null,"pay_time":null,"apply_refund_time":null,"actual_refund_time":null,"close_time":"2021-06-05T01:36:33Z","comment_status":0,"create_time":"2021-06-05T01:26:32Z","update_time":"2021-07-29T07:12:51Z","update_user_id":0,"bm_master_order_id":1400987366369402881},"after":null,"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1643252633000,"snapshot":"false","db":"mall_order","sequence":null,"table":"bm_order","server_id":1,"gtid":null,"file":"mysql-bin.000026","pos":4164480,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1643252633879,"transaction":null}
        UserBehavior userBehavior = new UserBehavior();
        DataChangeEvents<BmOrder> dataChangeEvents = JSON.parseObject(s, new TypeReference<DataChangeEvents<BmOrder>>() {
        });

        userBehavior.setEventTime(Instant.ofEpochMilli(dataChangeEvents.getTsMs()));

        // 新增下单行为
        if ("c".equals(dataChangeEvents.getOp())) {
            userBehavior.setBehaviorType(1);
            userBehavior.setMemberId(dataChangeEvents.getAfter().getMemberId());
            userBehavior.setTenantId(dataChangeEvents.getAfter().getTenantId());
            userBehavior.setAreaId(dataChangeEvents.getAfter().getAreaId());
        }

        if ("u".equals(dataChangeEvents.getOp())) {
            userBehavior.setBehaviorType(2);
            userBehavior.setMemberId(dataChangeEvents.getAfter().getMemberId());
            userBehavior.setTenantId(dataChangeEvents.getAfter().getTenantId());
            userBehavior.setAreaId(dataChangeEvents.getAfter().getAreaId());
        }

        if ("d".equals(dataChangeEvents.getOp())) {
            // FIXME
            userBehavior.setBehaviorType(3);
            userBehavior.setMemberId(dataChangeEvents.getBefore().getMemberId());
            userBehavior.setTenantId(dataChangeEvents.getBefore().getTenantId());
            userBehavior.setAreaId(dataChangeEvents.getBefore().getAreaId());
        }

        return userBehavior;
    }

    private static BmOrder toBmOrder(String s) {
        // {"before":null,"after":{"id":3,"tenant_id":1,"area_id":0,"overdue_time":"2021-06-05T01:36:32Z","buy_type":0,"transaction_type":3,"sharer_member_id":0,"member_id":1394220708019707906,"member_name":"李定辉","member_avatar_url":"https://file.cdn.tanchi.shop/dev/user/avatar/14a351adcb42edda7939d5123f916425","member_open_id":"oCOap5BHK-STBBILDcPx8ZATxE2M","member_mobile":"13267620116","member_notes":"","business_id":0,"business_name":"倍享话费充值","business_logo":"","product_id":0,"product_name":"测试产品1","product_pic":"","product_num":1,"product_price":"Cg==","total_money":"Cg==","pay_money":"Cg==","actual_pay_money":null,"discount_money":"AA==","order_status":4,"settle_status":0,"settle_time":null,"order_time":"2021-06-05T01:26:32Z","finish_time":null,"pay_time":null,"apply_refund_time":null,"actual_refund_time":null,"close_time":"2021-06-05T01:36:33Z","comment_status":0,"create_time":"2021-06-05T01:26:32Z","update_time":"2021-07-29T07:12:51Z","update_user_id":0,"bm_master_order_id":1400987366369402881},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1643252640000,"snapshot":"false","db":"mall_order","sequence":null,"table":"bm_order","server_id":1,"gtid":null,"file":"mysql-bin.000026","pos":4165168,"row":0,"thread":null,"query":null},"op":"c","ts_ms":1643252640690,"transaction":null}
        // {"before":{"id":3,"tenant_id":1,"area_id":0,"overdue_time":"2021-06-05T01:36:32Z","buy_type":0,"transaction_type":3,"sharer_member_id":0,"member_id":1394220708019707906,"member_name":"李定辉","member_avatar_url":"https://file.cdn.tanchi.shop/dev/user/avatar/14a351adcb42edda7939d5123f916425","member_open_id":"oCOap5BHK-STBBILDcPx8ZATxE2M","member_mobile":"13267620116","member_notes":"","business_id":0,"business_name":"倍享话费充值","business_logo":"","product_id":0,"product_name":"测试产品1","product_pic":"","product_num":1,"product_price":"Cg==","total_money":"Cg==","pay_money":"Cg==","actual_pay_money":null,"discount_money":"AA==","order_status":4,"settle_status":0,"settle_time":null,"order_time":"2021-06-05T01:26:32Z","finish_time":null,"pay_time":null,"apply_refund_time":null,"actual_refund_time":null,"close_time":"2021-06-05T01:36:33Z","comment_status":0,"create_time":"2021-06-05T01:26:32Z","update_time":"2021-07-29T07:12:51Z","update_user_id":0,"bm_master_order_id":1400987366369402881},"after":{"id":3,"tenant_id":1,"area_id":0,"overdue_time":"2021-06-05T01:36:32Z","buy_type":0,"transaction_type":3,"sharer_member_id":0,"member_id":1394220708019707906,"member_name":"fffff","member_avatar_url":"https://file.cdn.tanchi.shop/dev/user/avatar/14a351adcb42edda7939d5123f916425","member_open_id":"oCOap5BHK-STBBILDcPx8ZATxE2M","member_mobile":"13267620116","member_notes":"","business_id":0,"business_name":"倍享话费充值","business_logo":"","product_id":0,"product_name":"测试产品1","product_pic":"","product_num":1,"product_price":"Cg==","total_money":"Cg==","pay_money":"Cg==","actual_pay_money":null,"discount_money":"AA==","order_status":4,"settle_status":0,"settle_time":null,"order_time":"2021-06-05T01:26:32Z","finish_time":null,"pay_time":null,"apply_refund_time":null,"actual_refund_time":null,"close_time":"2021-06-05T01:36:33Z","comment_status":0,"create_time":"2021-06-05T01:26:32Z","update_time":"2022-01-27T03:29:36Z","update_user_id":0,"bm_master_order_id":1400987366369402881},"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1643254176000,"snapshot":"false","db":"mall_order","sequence":null,"table":"bm_order","server_id":1,"gtid":null,"file":"mysql-bin.000026","pos":4168693,"row":0,"thread":null,"query":null},"op":"u","ts_ms":1643254176375,"transaction":null}
        // {"before":{"id":1,"tenant_id":1,"area_id":0,"overdue_time":"2021-06-05T01:36:32Z","buy_type":0,"transaction_type":3,"sharer_member_id":0,"member_id":1394220708019707906,"member_name":"李定辉","member_avatar_url":"https://file.cdn.tanchi.shop/dev/user/avatar/14a351adcb42edda7939d5123f916425","member_open_id":"oCOap5BHK-STBBILDcPx8ZATxE2M","member_mobile":"13267620116","member_notes":"","business_id":0,"business_name":"倍享话费充值","business_logo":"","product_id":0,"product_name":"测试产品1","product_pic":"","product_num":1,"product_price":"Cg==","total_money":"Cg==","pay_money":"Cg==","actual_pay_money":null,"discount_money":"AA==","order_status":4,"settle_status":0,"settle_time":null,"order_time":"2021-06-05T01:26:32Z","finish_time":null,"pay_time":null,"apply_refund_time":null,"actual_refund_time":null,"close_time":"2021-06-05T01:36:33Z","comment_status":0,"create_time":"2021-06-05T01:26:32Z","update_time":"2021-07-29T07:12:51Z","update_user_id":0,"bm_master_order_id":1400987366369402881},"after":null,"source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1643252633000,"snapshot":"false","db":"mall_order","sequence":null,"table":"bm_order","server_id":1,"gtid":null,"file":"mysql-bin.000026","pos":4164480,"row":0,"thread":null,"query":null},"op":"d","ts_ms":1643252633879,"transaction":null}
        BmOrder bmOrder = new BmOrder();
        DataChangeEvents<BmOrder> dataChangeEvents = JSON.parseObject(s, new TypeReference<DataChangeEvents<BmOrder>>() {
        });

        // 新增下单行为
        if ("c".equals(dataChangeEvents.getOp())) {

        }

        if ("d".equals(dataChangeEvents.getOp())) {

        }


        return null;
    }

    public static class MySqlSink extends RichSinkFunction<UserBehavior> {

        private  PreparedStatement preparedStatement = null;
        private Connection connection = null;
        private String url = "jdbc:mysql://10.0.10.13:23100/mall_statistics";
        private String username = "root";
        private String password = "a123456";
        private String driverClassName = "com.mysql.cj.jdbc.Driver";

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Class.forName(driverClassName);
            connection = DriverManager.getConnection(url, username, password);
            preparedStatement = connection.prepareStatement("INSERT INTO user_behavior (member_id, event_time, behavior_type, tenant_id, area_id) VALUES (?, ?, ?, ?, ?)");
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }

        @Override
        public void invoke(UserBehavior value, Context context) throws Exception {
            preparedStatement.setLong(1, value.getMemberId());
            preparedStatement.setDate(2, new Date(value.getEventTime().toEpochMilli()));
            preparedStatement.setInt(3, value.getBehaviorType());
            preparedStatement.setInt(4, value.getTenantId());
            preparedStatement.setInt(5, value.getAreaId());
            preparedStatement.execute();
        }
    }
}
