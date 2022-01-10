package com.sf.bdp.flink.entity;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Map;

public class DynamicTopicSerialization implements KafkaSerializationSchema<Tuple2<String, byte[]>> {

    private Map<String, String> tableTopicMap;

    public DynamicTopicSerialization(Map<String, String> tableTopicMap) {
        this.tableTopicMap = tableTopicMap;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, byte[]> tuple2, @Nullable Long aLong) {
        String topicName = tableTopicMap.get(tuple2.f0);
        if (StringUtils.isBlank(topicName)) {
            return null;
        } else {
            return new ProducerRecord(topicName, tuple2.f1);
        }
    }


}