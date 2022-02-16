package com.boom.stream.userbehavior.deserializer;

import com.alibaba.fastjson.JSONObject;
import com.boom.stream.userbehavior.entity.UserBehavior;
import com.boom.stream.userbehavior.enums.UserBehaviorEnum;
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
 * @date 2022/2/15 19:42
 */
public class UserReviewBehaviorDebeziumDeserializer implements DebeziumDeserializationSchema<UserBehavior> {

    private static final long serialVersionUID = 1L;

    private transient JsonConverter jsonConverter;

    @Override
    public void deserialize(SourceRecord record, Collector<UserBehavior> out) throws Exception {
        if (jsonConverter == null) {
            jsonConverter = new JsonConverter();
            final HashMap<String, Object> configs = new HashMap<>(2);
            configs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
            configs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
            jsonConverter.configure(configs);
        }
        byte[] bytes =
                jsonConverter.fromConnectData(record.topic(), record.valueSchema(), record.value());
        String oplogString = new String(bytes);
        JSONObject oplog = JSONObject.parseObject(oplogString);

        if ("insert".equalsIgnoreCase(oplog.getString("operationType"))) {
            JSONObject fullDocument = oplog.getJSONObject("fullDocument");

            UserBehavior userBehavior = new UserBehavior();
            userBehavior.setSourceId(fullDocument.getJSONObject("_id").getString("$oid"));
            userBehavior.setTenantId(fullDocument.getInteger("tenant_id"));
            userBehavior.setAreaId(fullDocument.getInteger("area_id"));
            userBehavior.setMemberId(fullDocument.getJSONObject("member_id").getLong("$numberLong"));
            userBehavior.setEventTime(Instant.ofEpochMilli(fullDocument.getJSONObject("create_time").getLong("$date")));
            userBehavior.setBehaviorType(UserBehaviorEnum.COMMENT.getType());
            userBehavior.setBehaviorName(UserBehaviorEnum.COMMENT.getName());
            out.collect(userBehavior);
        }
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return BasicTypeInfo.of(UserBehavior.class);
    }
}
