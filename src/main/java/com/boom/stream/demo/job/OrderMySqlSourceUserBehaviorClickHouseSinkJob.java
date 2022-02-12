package com.boom.stream.demo.job;

import com.boom.stream.demo.entity.UserBehavior;
import com.boom.stream.demo.enums.OrderStatusEnum;
import com.boom.stream.demo.enums.UserBehaviorEnum;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/27 10:41
 */
public class OrderMySqlSourceUserBehaviorClickHouseSinkJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(3000L);

        TypeInformation<?>[] fieldTypes = new TypeInformation<?>[]{
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO,
                BasicTypeInfo.DATE_TYPE_INFO,
                BasicTypeInfo.DATE_TYPE_INFO,
                BasicTypeInfo.DATE_TYPE_INFO
        };
        String[] fieldNames = {"tenant_id", "area_id", "member_id", "order_status", "order_time", "pay_time", "update_time"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(fieldTypes, fieldNames);

        JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl("jdbc:mysql://10.0.10.13:23100/mall_order")
                .setUsername("root")
                .setPassword("a123456")
                .setQuery("SELECT tenant_id, area_id, member_id, order_status, order_time, pay_time, update_time FROM `order`")
                .setRowTypeInfo(rowTypeInfo)
                .finish();

        DataStreamSource<Row> dataStreamSource = env.createInput(jdbcInputFormat);
        dataStreamSource.name("order MySQL Source").setParallelism(4)
                .process(new UserBehaviorProcess())
                .addSink(JdbcSink.sink(
                        "INSERT INTO analyse.user_behavior_flink_cdc (tenant_id, area_id, member_id, event_time, behavior_type, behavior_name) VALUES (?, ?, ?, ?, ?, ?)",
                        (ps, t) -> {
                            ps.setInt(1, t.getTenantId());
                            ps.setInt(2, t.getAreaId());
                            ps.setLong(3, t.getMemberId());
                            ps.setString(4, t.getEventTime().atZone(ZoneId.of("+8")).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                            ps.setInt(5, t.getBehaviorType());
                            ps.setString(6, t.getBehaviorName());
                        },
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://10.0.10.13:25202/analyse?useUnicode=true&characterEncoding=UTF-8&useSSL=false&use_time_zone=UTC+8&use_server_time_zone=UTC+8")
                                .withUsername("default")
                                .withPassword("")
                                .withDriverName("cc.blynk.clickhouse.ClickHouseDriver")
                                .build()))
                .name("user_behavior_flink_cdc ClickHouse Sink")
                .setParallelism(2);

        env.execute("Order MySQL Source Print");
    }

    private static class UserBehaviorProcess extends ProcessFunction<Row, UserBehavior> {

        @Override
        public void processElement(Row row, Context ctx, Collector<UserBehavior> out) throws Exception {

            Integer orderStatus = row.getFieldAs("order_status");
            OrderStatusEnum orderStatusEnum = OrderStatusEnum.getEnumByCode(orderStatus);
            if (orderStatusEnum == null) {
                throw new RuntimeException("订单转换为行为记录失败，订单状态（order_status）错误，row：" + row);
            }

            UserBehavior userOrderBehavior = new UserBehavior();
            userOrderBehavior.setTenantId(row.getFieldAs("tenant_id"));
            userOrderBehavior.setAreaId(row.getFieldAs("area_id"));
            userOrderBehavior.setMemberId(row.getFieldAs("member_id"));
            userOrderBehavior.setEventTime(((Date) row.getFieldAs("order_time")).toInstant());
            userOrderBehavior.setBehaviorType(UserBehaviorEnum.ORDER.getType());
            userOrderBehavior.setBehaviorName(UserBehaviorEnum.ORDER.getName());

            UserBehavior userPayBehavior = new UserBehavior();
            userPayBehavior.setTenantId(row.getFieldAs("tenant_id"));
            userPayBehavior.setAreaId(row.getFieldAs("area_id"));
            userPayBehavior.setMemberId(row.getFieldAs("member_id"));
            userPayBehavior.setBehaviorType(UserBehaviorEnum.PAY.getType());
            userPayBehavior.setBehaviorName(UserBehaviorEnum.PAY.getName());


            UserBehavior userUseBehavior = new UserBehavior();
            userUseBehavior.setTenantId(row.getFieldAs("tenant_id"));
            userUseBehavior.setAreaId(row.getFieldAs("area_id"));
            userUseBehavior.setMemberId(row.getFieldAs("member_id"));
            userUseBehavior.setBehaviorType(UserBehaviorEnum.USE.getType());
            userUseBehavior.setBehaviorName(UserBehaviorEnum.USE.getName());


            switch (orderStatusEnum) {
                case WAIT_PAY:
                    out.collect(userOrderBehavior);
                    break;
                case WAIT_CHECK:
                    out.collect(userOrderBehavior);
                    userPayBehavior.setEventTime(((Date) row.getFieldAs("pay_time")).toInstant());
                    out.collect(userPayBehavior);
                    break;
                case ALREADY_CHECK:
                    out.collect(userOrderBehavior);
                    userPayBehavior.setEventTime(((Date) row.getFieldAs("pay_time")).toInstant());
                    out.collect(userPayBehavior);
                    userUseBehavior.setEventTime(((Date) row.getFieldAs("update_time")).toInstant());
                    out.collect(userUseBehavior);
                    break;
                case EXPIRED:
                    out.collect(userOrderBehavior);
                    userPayBehavior.setEventTime(((Date) row.getFieldAs("pay_time")).toInstant());
                    out.collect(userPayBehavior);
                    break;
                case CLOSED:
                    out.collect(userOrderBehavior);
                    break;
                default:
                    throw new RuntimeException("订单转换为行为记录失败，row：" + row);
            }
        }
    }
}
