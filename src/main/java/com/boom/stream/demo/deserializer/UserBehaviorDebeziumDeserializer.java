package com.boom.stream.demo.deserializer;

import com.alibaba.fastjson.JSONObject;
import com.boom.stream.demo.entity.UserBehavior;
import com.boom.stream.demo.enums.*;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.Date;
import java.util.HashMap;
import java.util.Optional;

public class UserBehaviorDebeziumDeserializer implements DebeziumDeserializationSchema<UserBehavior> {

    private static final long serialVersionUID = 1L;

    private transient JsonConverter jsonConverter;

    @Override
    public void deserialize(SourceRecord record, Collector<UserBehavior> out) {

        try {
            if (jsonConverter == null) {
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

            if ("mysql_binlog_source.mall_order.order".equals(record.topic())) {
                dealOrderSource(binlog, out);
                return;
            }

            if ("mysql_binlog_source.member.my_collect".equals(record.topic())) {
                dealMyCollectSource(binlog, out);
                return;
            }

            if ("mysql_binlog_source.mall_merchant.overlord_meal_participate_record".equals(record.topic())) {
                dealOverlordMealSource(binlog, out);
            }

        } catch (Exception e) {
            throw e;
        }

    }

    private void dealOverlordMealSource(JSONObject binlog, Collector<UserBehavior> out) {
        JSONObject after = binlog.getJSONObject("after");

        if ("r".equals(binlog.getString("op")) || "c".equals(binlog.getString("op"))) {
            UserBehavior userBehavior = new UserBehavior();
            userBehavior.setSourceId(after.getString("id"));
            userBehavior.setTenantId(after.getInteger("tenant_id"));
            userBehavior.setAreaId(after.getInteger("area_id"));
            userBehavior.setMemberId(after.getLong("member_id"));
            Date createTime = after.getDate("create_time");
            if (createTime == null) {
                return;
            }
            userBehavior.setEventTime(createTime.toInstant());
            userBehavior.setBehaviorType(UserBehaviorEnum.PARTICIPATE_IN_OVERLORD_MEAL.getType());
            userBehavior.setBehaviorName(UserBehaviorEnum.PARTICIPATE_IN_OVERLORD_MEAL.getName());
            out.collect(userBehavior);
        }
    }

    private void dealMyCollectSource(JSONObject binlog, Collector<UserBehavior> out) {
        JSONObject before = binlog.getJSONObject("before");
        JSONObject after = binlog.getJSONObject("after");

        Optional<CollectTypeEnum> collectTypeOptional = CollectTypeEnum.of(after.getInteger("collect_type"));
        if (collectTypeOptional.isPresent()) {
            CollectTypeEnum collectTypeEnum = collectTypeOptional.get();

            if ("r".equals(binlog.getString("op"))) {
                if (DeleteStatusEnum.DELETED.getCode().equals(after.getInteger("delete_status"))) {
                    // 记录收藏和取消收藏
                    UserBehavior userCollectBehavior = new UserBehavior();
                    userCollectBehavior.setSourceId(after.getString("id"));
                    userCollectBehavior.setTenantId(after.getInteger("tenant_id"));
                    userCollectBehavior.setAreaId(after.getInteger("area_id"));
                    userCollectBehavior.setMemberId(after.getLong("member_id"));
                    userCollectBehavior.setEventTime(after.getDate("create_time").toInstant());
                    UserBehavior userUnCollectBehavior = new UserBehavior();
                    userUnCollectBehavior.setSourceId(after.getString("id"));
                    userUnCollectBehavior.setTenantId(after.getInteger("tenant_id"));
                    userUnCollectBehavior.setAreaId(after.getInteger("area_id"));
                    userUnCollectBehavior.setMemberId(after.getLong("member_id"));
                    userUnCollectBehavior.setEventTime(after.getDate("update_time").toInstant());
                    switch (collectTypeEnum) {
                        case PRODUCT:
                            userCollectBehavior.setBehaviorType(UserBehaviorEnum.COLLECT_PRODUCT.getType());
                            userCollectBehavior.setBehaviorName(UserBehaviorEnum.COLLECT_PRODUCT.getName());
                            userUnCollectBehavior.setBehaviorType(UserBehaviorEnum.UN_COLLECT_PRODUCT.getType());
                            userUnCollectBehavior.setBehaviorName(UserBehaviorEnum.UN_COLLECT_PRODUCT.getName());
                            out.collect(userCollectBehavior);
                            out.collect(userUnCollectBehavior);
                            break;
                        case STORE:
                            userCollectBehavior.setBehaviorType(UserBehaviorEnum.COLLECT_STORE.getType());
                            userCollectBehavior.setBehaviorName(UserBehaviorEnum.COLLECT_STORE.getName());
                            userUnCollectBehavior.setBehaviorType(UserBehaviorEnum.UN_COLLECT_STORE.getType());
                            userUnCollectBehavior.setBehaviorName(UserBehaviorEnum.UN_COLLECT_STORE.getName());
                            out.collect(userCollectBehavior);
                            out.collect(userUnCollectBehavior);
                            break;
                        default:
                    }
                    return;
                }

                if (DeleteStatusEnum.NOT_DELETED.getCode().equals(after.getInteger("delete_status"))) {
                    // 记录收藏
                    UserBehavior userCollectBehavior = new UserBehavior();
                    userCollectBehavior.setSourceId(after.getString("id"));
                    userCollectBehavior.setTenantId(after.getInteger("tenant_id"));
                    userCollectBehavior.setAreaId(after.getInteger("area_id"));
                    userCollectBehavior.setMemberId(after.getLong("member_id"));
                    userCollectBehavior.setEventTime(after.getDate("create_time").toInstant());
                    switch (collectTypeEnum) {
                        case PRODUCT:
                            userCollectBehavior.setBehaviorType(UserBehaviorEnum.COLLECT_PRODUCT.getType());
                            userCollectBehavior.setBehaviorName(UserBehaviorEnum.COLLECT_PRODUCT.getName());
                            out.collect(userCollectBehavior);
                            break;
                        case STORE:
                            userCollectBehavior.setBehaviorType(UserBehaviorEnum.COLLECT_STORE.getType());
                            userCollectBehavior.setBehaviorName(UserBehaviorEnum.COLLECT_STORE.getName());
                            out.collect(userCollectBehavior);
                            break;
                        default:
                    }
                }
                return;
            }

            if ("c".equals(binlog.getString("op"))) {
                UserBehavior userCollectBehavior = new UserBehavior();
                userCollectBehavior.setSourceId(after.getString("id"));
                userCollectBehavior.setTenantId(after.getInteger("tenant_id"));
                userCollectBehavior.setAreaId(after.getInteger("area_id"));
                userCollectBehavior.setMemberId(after.getLong("member_id"));
                userCollectBehavior.setEventTime(after.getDate("create_time").toInstant());

                switch (collectTypeEnum) {
                    case PRODUCT:
                        userCollectBehavior.setBehaviorType(UserBehaviorEnum.COLLECT_PRODUCT.getType());
                        userCollectBehavior.setBehaviorName(UserBehaviorEnum.COLLECT_PRODUCT.getName());
                        out.collect(userCollectBehavior);
                        break;
                    case STORE:
                        userCollectBehavior.setBehaviorType(UserBehaviorEnum.COLLECT_STORE.getType());
                        userCollectBehavior.setBehaviorName(UserBehaviorEnum.COLLECT_STORE.getName());
                        out.collect(userCollectBehavior);
                        break;
                    default:
                }
                return;
            }

            if ("u".equals(binlog.getString("op"))) {
                Integer beforeDeleteStatus = before.getInteger("delete_status");
                Integer afterDeleteStatus = after.getInteger("delete_status");
                if (beforeDeleteStatus.equals(afterDeleteStatus)) {
                    return;
                }

                UserBehavior userUnCollectBehavior = new UserBehavior();
                userUnCollectBehavior.setSourceId(after.getString("id"));
                userUnCollectBehavior.setTenantId(after.getInteger("tenant_id"));
                userUnCollectBehavior.setAreaId(after.getInteger("area_id"));
                userUnCollectBehavior.setMemberId(after.getLong("member_id"));
                userUnCollectBehavior.setEventTime(after.getDate("update_time").toInstant());
                switch (collectTypeEnum) {
                    case PRODUCT:
                        userUnCollectBehavior.setBehaviorType(UserBehaviorEnum.COLLECT_PRODUCT.getType());
                        userUnCollectBehavior.setBehaviorName(UserBehaviorEnum.COLLECT_PRODUCT.getName());
                        out.collect(userUnCollectBehavior);
                        break;
                    case STORE:
                        userUnCollectBehavior.setBehaviorType(UserBehaviorEnum.COLLECT_STORE.getType());
                        userUnCollectBehavior.setBehaviorName(UserBehaviorEnum.COLLECT_STORE.getName());
                        out.collect(userUnCollectBehavior);
                        break;
                    default:
                }
            }
        }
    }

    private void dealOrderSource(JSONObject binlog, Collector<UserBehavior> out) {
        JSONObject before = binlog.getJSONObject("before");
        JSONObject after = binlog.getJSONObject("after");

        if ("r".equals(binlog.getString("op"))) {
            Integer orderStatus = after.getInteger("order_status");
            OrderStatusEnum orderStatusEnum = OrderStatusEnum.getEnumByCode(orderStatus);
            if (orderStatusEnum == null) {
                throw new RuntimeException("订单转换为行为记录失败，订单状态（order_status）错误，binlog：" + binlog);
            }

            UserBehavior userOrderBehavior = new UserBehavior();
            userOrderBehavior.setSourceId(after.getString("id"));
            userOrderBehavior.setTenantId(after.getInteger("tenant_id"));
            userOrderBehavior.setAreaId(after.getInteger("area_id"));
            userOrderBehavior.setMemberId(after.getLong("member_id"));
            userOrderBehavior.setEventTime(after.getDate("order_time").toInstant());
            userOrderBehavior.setBehaviorType(UserBehaviorEnum.ORDER.getType());
            userOrderBehavior.setBehaviorName(UserBehaviorEnum.ORDER.getName());

            UserBehavior userPayBehavior = new UserBehavior();
            userPayBehavior.setSourceId(after.getString("id"));
            userPayBehavior.setTenantId(after.getInteger("tenant_id"));
            userPayBehavior.setAreaId(after.getInteger("area_id"));
            userPayBehavior.setMemberId(after.getLong("member_id"));
            userPayBehavior.setBehaviorType(UserBehaviorEnum.PAY.getType());
            userPayBehavior.setBehaviorName(UserBehaviorEnum.PAY.getName());

            UserBehavior userUseBehavior = new UserBehavior();
            userUseBehavior.setSourceId(after.getString("id"));
            userUseBehavior.setTenantId(after.getInteger("tenant_id"));
            userUseBehavior.setAreaId(after.getInteger("area_id"));
            userUseBehavior.setMemberId(after.getLong("member_id"));
            userUseBehavior.setBehaviorType(UserBehaviorEnum.USE.getType());
            userUseBehavior.setBehaviorName(UserBehaviorEnum.USE.getName());

            Date payTime = after.getDate("pay_time");
            Date updateTime = after.getDate("update_time");

            switch (orderStatusEnum) {
                case WAIT_PAY:
                    out.collect(userOrderBehavior);
                    break;
                case WAIT_CHECK:
                    out.collect(userOrderBehavior);
                    if (payTime == null) {
                        break;
                    }
                    userPayBehavior.setEventTime(payTime.toInstant());
                    out.collect(userPayBehavior);
                    break;
                case ALREADY_CHECK:
                    out.collect(userOrderBehavior);
                    if (payTime == null) {
                        break;
                    }
                    userPayBehavior.setEventTime(payTime.toInstant());
                    out.collect(userPayBehavior);
                    if (updateTime == null) {
                        break;
                    }
                    userUseBehavior.setEventTime(updateTime.toInstant());
                    out.collect(userUseBehavior);
                    break;
                case EXPIRED:
                    out.collect(userOrderBehavior);
                    if (payTime == null) {
                        break;
                    }
                    userPayBehavior.setEventTime(payTime.toInstant());
                    out.collect(userPayBehavior);
                    break;
                case CLOSED:
                    out.collect(userOrderBehavior);
                    break;
                default:
                    throw new RuntimeException("订单转换为行为记录失败，binlog：" + binlog);
            }


            Integer refundStatus = after.getInteger("refund_status");
            RefundStatusEnum refundStatusEnum = RefundStatusEnum.getEnumByCode(refundStatus);
            if (refundStatusEnum == null) {
                throw new RuntimeException("订单转换为行为记录失败，退款状态（refund_status）错误，binlog：" + binlog);
            }

            if (!refundStatusEnum.equals(RefundStatusEnum.NOT_ALL_REFUNDED)) {
                UserBehavior userRefundBehavior = new UserBehavior();
                userRefundBehavior.setSourceId(after.getString("id"));
                userRefundBehavior.setTenantId(after.getInteger("tenant_id"));
                userRefundBehavior.setAreaId(after.getInteger("area_id"));
                userRefundBehavior.setMemberId(after.getLong("member_id"));
                Date refundTime = after.getDate("update_time");
                if (refundTime == null) {
                    return;
                }
                userRefundBehavior.setEventTime(refundTime.toInstant());
                userRefundBehavior.setBehaviorType(UserBehaviorEnum.REFUND.getType());
                userRefundBehavior.setBehaviorName(UserBehaviorEnum.REFUND.getName());
                out.collect(userRefundBehavior);
            }
            return;
        }

        if ("c".equals(binlog.getString("op"))) {
            UserBehavior userOrderBehavior = new UserBehavior();
            userOrderBehavior.setSourceId(after.getString("id"));
            userOrderBehavior.setTenantId(after.getInteger("tenant_id"));
            userOrderBehavior.setAreaId(after.getInteger("area_id"));
            userOrderBehavior.setMemberId(after.getLong("member_id"));
            userOrderBehavior.setEventTime(after.getDate("order_time").toInstant());
            userOrderBehavior.setBehaviorType(UserBehaviorEnum.ORDER.getType());
            userOrderBehavior.setBehaviorName(UserBehaviorEnum.ORDER.getName());
            out.collect(userOrderBehavior);
            return;
        }

        if ("u".equals(binlog.getString("op"))) {
            Integer beforeOrderStatus = before.getInteger("order_status");
            Integer afterOrderStatus = after.getInteger("order_status");
            if (!beforeOrderStatus.equals(afterOrderStatus)) {
                OrderStatusEnum orderStatusEnum = OrderStatusEnum.getEnumByCode(afterOrderStatus);
                if (orderStatusEnum == null) {
                    throw new RuntimeException("订单转换为行为记录失败，订单状态（order_status）错误，binlog：" + binlog);
                }
                recordOrder(orderStatusEnum, after, out);
                return;
            }

            Integer beforeRefundStatus = before.getInteger("refund_status");
            Integer afterRefundStatus = after.getInteger("refund_status");
            if (!beforeRefundStatus.equals(afterRefundStatus)) {
                RefundStatusEnum refundStatusEnum = RefundStatusEnum.getEnumByCode(afterRefundStatus);
                if (refundStatusEnum == null) {
                    throw new RuntimeException("订单转换为行为记录失败，退款状态（refund_status）错误，binlog：" + binlog);
                }
                recordRefund(refundStatusEnum, after, out);
            }
        }
    }

    private void recordRefund(RefundStatusEnum refundStatusEnum, JSONObject after, Collector<UserBehavior> out) {
        if (refundStatusEnum.equals(RefundStatusEnum.ALL_APPLY_REFUND)) {
            UserBehavior userPayBehavior = new UserBehavior();
            userPayBehavior.setSourceId(after.getString("id"));
            userPayBehavior.setTenantId(after.getInteger("tenant_id"));
            userPayBehavior.setAreaId(after.getInteger("area_id"));
            userPayBehavior.setMemberId(after.getLong("member_id"));
            Date refundTime = after.getDate("update_time");
            if (refundTime == null) {
                return;
            }
            userPayBehavior.setEventTime(refundTime.toInstant());
            userPayBehavior.setBehaviorType(UserBehaviorEnum.REFUND.getType());
            userPayBehavior.setBehaviorName(UserBehaviorEnum.REFUND.getName());
            out.collect(userPayBehavior);
        }
    }

    private void recordOrder(OrderStatusEnum orderStatusEnum, JSONObject after, Collector<UserBehavior> out) {
        switch (orderStatusEnum) {
            case WAIT_CHECK:
                UserBehavior userPayBehavior = new UserBehavior();
                userPayBehavior.setSourceId(after.getString("id"));
                userPayBehavior.setTenantId(after.getInteger("tenant_id"));
                userPayBehavior.setAreaId(after.getInteger("area_id"));
                userPayBehavior.setMemberId(after.getLong("member_id"));
                Date payTime = after.getDate("pay_time");
                if (payTime == null) {
                    return;
                }
                userPayBehavior.setEventTime(payTime.toInstant());
                userPayBehavior.setBehaviorType(UserBehaviorEnum.PAY.getType());
                userPayBehavior.setBehaviorName(UserBehaviorEnum.PAY.getName());
                out.collect(userPayBehavior);
                break;
            case ALREADY_CHECK:
                UserBehavior userUseBehavior = new UserBehavior();
                userUseBehavior.setSourceId(after.getString("id"));
                userUseBehavior.setTenantId(after.getInteger("tenant_id"));
                userUseBehavior.setAreaId(after.getInteger("area_id"));
                userUseBehavior.setMemberId(after.getLong("member_id"));
                Date updateTime = after.getDate("update_time");
                if (updateTime == null) {
                    return;
                }
                userUseBehavior.setEventTime(updateTime.toInstant());
                userUseBehavior.setBehaviorType(UserBehaviorEnum.USE.getType());
                userUseBehavior.setBehaviorName(UserBehaviorEnum.USE.getName());
                out.collect(userUseBehavior);
                break;
            default:
                throw new RuntimeException("非法订单状态");
        }
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return BasicTypeInfo.of(UserBehavior.class);
    }
}