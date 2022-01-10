package com.sf.bdp.flink.extractor;

import com.sf.bdp.flink.entity.GenericRowRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

public class ProducerRecordExtractor implements RecordExtractor<Tuple2<String, GenericRowRecord>, ProducerRecord<byte[], byte[]>> {

    private final Map<String, String> tableTopicMap;

    public ProducerRecordExtractor(Map<String, String> tableTopicMap) {
        this.tableTopicMap = tableTopicMap;
    }

    @Override
    public ProducerRecord<byte[], byte[]> apply(Tuple2<String, GenericRowRecord> stringGenericRowRecordTuple2) {

        return null;
    }

}
