package com.sf.bdp.serialization;

import com.sf.bdp.record.GenericCdcRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class GenericKafkaSerializationSchema implements KafkaSerializationSchema<Tuple2<String, GenericCdcRecord>> {

    protected final Map<String, String> tableTopicMap;

    public GenericKafkaSerializationSchema(Map<String, String> tableTopicMap) {
        this.tableTopicMap = tableTopicMap;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, GenericCdcRecord> tuple2, @Nullable Long aLong) {
        GenericCdcRecord record = tuple2.f1;
        String topicName = tableTopicMap.get(record.getDbTable());
        if (StringUtils.isNotBlank(topicName)) {
            return new ProducerRecord(topicName, record.toString().getBytes(StandardCharsets.UTF_8));
        }
        return null;

    }
}