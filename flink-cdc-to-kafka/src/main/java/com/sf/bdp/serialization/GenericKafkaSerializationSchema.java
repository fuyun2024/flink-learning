package com.sf.bdp.serialization;

import com.sf.bdp.entity.GenericCdcRecord;
import com.sf.bdp.entity.GenericAvroRecord;
import com.sf.bdp.extractor.RecordExtractor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Map;

public class GenericKafkaSerializationSchema implements KafkaSerializationSchema<Tuple2<String, GenericCdcRecord>> {

    protected final Map<String, String> tableTopicMap;
    private final SerializationSchema<GenericAvroRecord> serializationSchema;
    private final RecordExtractor<GenericCdcRecord, GenericAvroRecord> recordExtractor;

    public GenericKafkaSerializationSchema(Map<String, String> tableTopicMap,
                                           SerializationSchema<GenericAvroRecord> serializationSchema,
                                           RecordExtractor<GenericCdcRecord, GenericAvroRecord> recordExtractor) {
        this.tableTopicMap = tableTopicMap;
        this.serializationSchema = serializationSchema;
        this.recordExtractor = recordExtractor;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, GenericCdcRecord> tuple2, @Nullable Long aLong) {
        GenericCdcRecord record = tuple2.f1;
        String topicName = tableTopicMap.get(record.getDbTable());
        if (StringUtils.isNotBlank(topicName)) {
            // recordExtractor
            GenericAvroRecord genericAvroRecord = recordExtractor.apply(record);
            genericAvroRecord.setTopicName(topicName);

            // serialization
            byte[] bytes = serializationSchema.serialize(genericAvroRecord);

            return new ProducerRecord(topicName, bytes);
        }
        return null;

    }
}