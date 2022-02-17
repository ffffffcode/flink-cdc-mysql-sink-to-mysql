package com.boom.stream.userbehavior.deserializer;

import com.alibaba.fastjson.JSONObject;
import com.boom.stream.userbehavior.entity.UserBehavior;
import com.boom.stream.userbehavior.enums.*;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Optional;

@Slf4j
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

            // 下单、支付
            if ("mysql_binlog_source.mall_order.order".equals(record.topic())) {
                dealOrderSource(binlog, out);
                return;
            }

            // 核销
            if ("mysql_binlog_source.mall_order.order_check_code".equals(record.topic())) {
                dealOrderCheckCodeSource(binlog, out);
                return;
            }

            // 退款
            if ("mysql_binlog_source.mall_order.order_refund".equals(record.topic())) {
                dealOrderRefundSource(binlog, out);
                return;
            }

            // 收藏
            if ("mysql_binlog_source.member.my_collect".equals(record.topic())) {
                dealMyCollectSource(binlog, out);
                return;
            }

            // 霸王餐
            if ("mysql_binlog_source.mall_merchant.overlord_meal_participate_record".equals(record.topic())) {
                dealOverlordMealSource(binlog, out);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void dealOrderRefundSource(JSONObject binlog, Collector<UserBehavior> out) {
        JSONObject after = binlog.getJSONObject("after");
        if ("r".equals(binlog.getString("op")) || "u".equals(binlog.getString("op"))) {
            Integer applyRefundType = after.getInteger("apply_refund_type");
            Integer verifyStatus = after.getInteger("verify_status");

            ApplyRefundTypeEnum applyRefundTypeEnum = ApplyRefundTypeEnum.getEnumByCode(applyRefundType);
            OrderRefundVerifyStatusEnum orderRefundVerifyStatusEnum = OrderRefundVerifyStatusEnum.getEnumByCode(verifyStatus);

            // 审核通过 且 不是第三方处理失败自动退款 且 退款成功的订单
            Date realityRefundTime = after.getDate("reality_refund_time");
            boolean dealCondition = OrderRefundVerifyStatusEnum.PASS.equals(orderRefundVerifyStatusEnum)
                    && !ApplyRefundTypeEnum.THIRD_PART_FAIL.equals(applyRefundTypeEnum)
                    && realityRefundTime != null;

            if (dealCondition) {
                UserBehavior userRefundBehavior = new UserBehavior();
                userRefundBehavior.setSourceId(after.getString("id"));
                userRefundBehavior.setTenantId(after.getInteger("tenant_id"));
                userRefundBehavior.setAreaId(after.getInteger("area_id"));
                userRefundBehavior.setMemberId(after.getLong("member_id"));
                userRefundBehavior.setBehaviorType(UserBehaviorEnum.REFUND.getType());
                userRefundBehavior.setBehaviorName(UserBehaviorEnum.REFUND.getName());
                userRefundBehavior.setEventTime(realityRefundTime.toInstant());
                out.collect(userRefundBehavior);
            }
        }
    }

    private void dealOrderCheckCodeSource(JSONObject binlog, Collector<UserBehavior> out) {
        JSONObject after = binlog.getJSONObject("after");
        if ("r".equals(binlog.getString("op")) || "u".equals(binlog.getString("op"))) {
            Integer checkStatus = after.getInteger("check_status");
            CheckStatusEnum checkStatusEnum = CheckStatusEnum.getEnumByCode(checkStatus);
            if (CheckStatusEnum.USED.equals(checkStatusEnum)) {
                UserBehavior userUseBehavior = new UserBehavior();
                userUseBehavior.setSourceId(after.getString("id"));
                userUseBehavior.setTenantId(after.getInteger("tenant_id"));
                userUseBehavior.setAreaId(after.getInteger("area_id"));
                userUseBehavior.setMemberId(after.getLong("member_id"));
                userUseBehavior.setBehaviorType(UserBehaviorEnum.USE.getType());
                userUseBehavior.setBehaviorName(UserBehaviorEnum.USE.getName());
                Date checkTime = after.getDate("check_time");
                if (checkTime == null) {
                    return;
                }
                userUseBehavior.setEventTime(checkTime.toInstant());
                out.collect(userUseBehavior);
            }
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
        JSONObject after = binlog.getJSONObject("after");

        if ("r".equals(binlog.getString("op"))) {
            Integer orderStatus = after.getInteger("order_status");
            OrderStatusEnum orderStatusEnum = OrderStatusEnum.getEnumByCode(orderStatus);
            if (orderStatusEnum == null) {
                log.warn("订单转换为行为记录失败，订单状态（order_status）错误，binlog：" + binlog);
                return;
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

            Date payTime = after.getDate("pay_time");
            BigDecimal actualPayMoney = after.getBigDecimal("actual_pay_money");

            switch (orderStatusEnum) {
                case WAIT_PAY:
                    out.collect(userOrderBehavior);
                    break;
                case WAIT_CHECK:
                    out.collect(userOrderBehavior);
                    if (payTime == null || actualPayMoney == null) {
                        break;
                    }
                    userPayBehavior.setEventTime(payTime.toInstant());
                    userPayBehavior.setActualPayMoney(actualPayMoney);
                    out.collect(userPayBehavior);
                    break;
                case EXPIRED:
                    out.collect(userOrderBehavior);
                    if (payTime == null || actualPayMoney == null) {
                        break;
                    }
                    userPayBehavior.setEventTime(payTime.toInstant());
                    userPayBehavior.setActualPayMoney(actualPayMoney);
                    out.collect(userPayBehavior);
                    break;
                case CLOSED:
                    out.collect(userOrderBehavior);
                    break;
                default:
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
            Integer afterOrderStatus = after.getInteger("order_status");
            OrderStatusEnum orderStatusEnum = OrderStatusEnum.getEnumByCode(afterOrderStatus);
            if (OrderStatusEnum.WAIT_CHECK.equals(orderStatusEnum)) {
                UserBehavior userPayBehavior = new UserBehavior();
                userPayBehavior.setSourceId(after.getString("id"));
                userPayBehavior.setTenantId(after.getInteger("tenant_id"));
                userPayBehavior.setAreaId(after.getInteger("area_id"));
                userPayBehavior.setMemberId(after.getLong("member_id"));
                userPayBehavior.setBehaviorType(UserBehaviorEnum.PAY.getType());
                userPayBehavior.setBehaviorName(UserBehaviorEnum.PAY.getName());

                Date payTime = after.getDate("pay_time");
                BigDecimal actualPayMoney = after.getBigDecimal("actual_pay_money");
                if (payTime == null || actualPayMoney == null) {
                    return;
                }
                userPayBehavior.setEventTime(payTime.toInstant());
                userPayBehavior.setActualPayMoney(actualPayMoney);
                out.collect(userPayBehavior);
            }
        }
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return BasicTypeInfo.of(UserBehavior.class);
    }
}