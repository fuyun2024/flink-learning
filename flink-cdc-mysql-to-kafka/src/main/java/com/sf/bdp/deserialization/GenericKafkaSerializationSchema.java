package com.sf.bdp.deserialization;

import com.sf.bdp.extractor.RecordExtractor;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class GenericKafkaSerializationSchema<In> implements KafkaSerializationSchema<In> {

    private final RecordExtractor<In, ProducerRecord<byte[], byte[]>> recordExtractor;

    public GenericKafkaSerializationSchema(RecordExtractor<In, ProducerRecord<byte[], byte[]>> recordExtractor) {
        this.recordExtractor = recordExtractor;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(In record, @Nullable Long aLong) {
        return recordExtractor.apply(record);
    }

}