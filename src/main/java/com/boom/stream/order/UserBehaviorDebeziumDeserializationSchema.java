package com.boom.stream.order;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.boom.stream.order.entity.BmOrder;
import com.boom.stream.demo.entity.UserBehavior;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.time.Instant;
import java.util.HashMap;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/27 16:52
 */
public class UserBehaviorDebeziumDeserializationSchema implements DebeziumDeserializationSchema<UserBehavior> {

    private transient JsonConverter jsonConverter;

    @Override
    public void deserialize(SourceRecord record, Collector<UserBehavior> out) throws Exception {


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


        UserBehavior userBehavior = new UserBehavior();
        DataChangeEvents<BmOrder> dataChangeEvents = JSON.parseObject(new String(bytes), new TypeReference<DataChangeEvents<BmOrder>>() {
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

        out.collect(userBehavior);
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return BasicTypeInfo.of(UserBehavior.class);
    }
}
