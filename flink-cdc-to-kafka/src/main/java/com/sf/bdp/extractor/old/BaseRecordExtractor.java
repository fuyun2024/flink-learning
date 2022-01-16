package com.sf.bdp.extractor.old;

import com.sf.bdp.entity.GenericCdcRecord;
import com.sf.bdp.extractor.RecordExtractor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

public abstract class BaseRecordExtractor implements RecordExtractor<Tuple2<String, GenericCdcRecord>, ProducerRecord<byte[], byte[]>> {

    protected final Map<String, String> tableTopicMap;

    public BaseRecordExtractor(Map<String, String> tableTopicMap) {
        this.tableTopicMap = tableTopicMap;
    }

    @Override
    public ProducerRecord<byte[], byte[]> apply(Tuple2<String, GenericCdcRecord> tuple2) {
        GenericCdcRecord record = tuple2.f1;
        String topicName = getTopicName(record.getDbTable());
        if (StringUtils.isNotBlank(topicName)) {
            return new ProducerRecord(topicName, extractorRecord(record));
        }
        return null;
    }

    public String getTopicName(String dbTable) {
        return tableTopicMap.get(dbTable);
    }

    public String getValueSubject(String dbTable) {
        return getTopicName(dbTable) + "-value";
    }


    /**
     * 提取数据
     *
     * @param record
     * @return
     */
    protected abstract byte[] extractorRecord(GenericCdcRecord record);


}
