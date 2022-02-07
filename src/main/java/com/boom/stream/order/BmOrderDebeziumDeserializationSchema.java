package com.boom.stream.order;

import com.alibaba.fastjson.JSONObject;
import com.boom.stream.order.entity.BmOrder;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;

/**
 * @author aaron
 * @version 1.0
 * @date 2022/1/26 16:18
 */
public class BmOrderDebeziumDeserializationSchema implements DebeziumDeserializationSchema<DataChangeEvents> {

    private static final long serialVersionUID = 1L;

    private transient JsonConverter jsonConverter;

    @Override
    public void deserialize(SourceRecord record, Collector<DataChangeEvents> out) throws Exception {
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
        String recordJsonString = new String(bytes);
        JSONObject jsonObject = JSONObject.parseObject(recordJsonString);
        DataChangeEvents<BmOrder> dataChangeEvents = new DataChangeEvents<>();
        dataChangeEvents.setOp(jsonObject.getString("op"));
        dataChangeEvents.setAfter(jsonObject.getObject("after", BmOrder.class));
        dataChangeEvents.setBefore(jsonObject.getObject("before", BmOrder.class));
        dataChangeEvents.setTsMs(jsonObject.getLong("ts_ms"));
        out.collect(dataChangeEvents);
    }

    @Override
    public TypeInformation<DataChangeEvents> getProducedType() {
        return BasicTypeInfo.of(DataChangeEvents.class);
    }

}
