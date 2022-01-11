package com.sf.bdp.extractor;

import com.sf.bdp.entity.GenericRowRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class JsonProducerRecordExtractor extends BaseProducerRecordExtractor {


    public JsonProducerRecordExtractor(Map<String, String> tableTopicMap) {
        super(tableTopicMap);
    }

    @Override
    public ProducerRecord<byte[], byte[]> apply(Tuple2<String, GenericRowRecord> tuple2) {
        GenericRowRecord record = tuple2.f1;
        String topicName = tableTopicMap.get(record.getDbTable());
        if (StringUtils.isNotBlank(topicName)) {
            /**
             * 把对象转换成 json
             */
            return new ProducerRecord(topicName, record.toString().getBytes(StandardCharsets.UTF_8));
        }
        return null;
    }

}
