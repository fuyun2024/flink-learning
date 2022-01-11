package com.sf.bdp.extractor;

import com.sf.bdp.entity.GenericRowRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class BaseProducerRecordExtractor implements RecordExtractor<Tuple2<String, GenericRowRecord>, ProducerRecord<byte[], byte[]>> {

    protected final Map<String, String> tableTopicMap;

    public BaseProducerRecordExtractor(Map<String, String> tableTopicMap) {
        this.tableTopicMap = tableTopicMap;
    }

    @Override
    public ProducerRecord<byte[], byte[]> apply(Tuple2<String, GenericRowRecord> tuple2) {
        GenericRowRecord record = tuple2.f1;
        String topicName = tableTopicMap.get(record.getDbTable());
        if (StringUtils.isNotBlank(topicName)) {
            return new ProducerRecord(topicName, record.toString().getBytes(StandardCharsets.UTF_8));
        }
        return null;
    }

}
