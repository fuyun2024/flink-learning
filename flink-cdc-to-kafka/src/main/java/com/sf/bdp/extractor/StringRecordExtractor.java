package com.sf.bdp.extractor;

import com.sf.bdp.entity.GenericRowRecord;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class StringRecordExtractor extends BaseProducerRecordExtractor {


    public StringRecordExtractor(Map<String, String> tableTopicMap) {
        super(tableTopicMap);
    }

    @Override
    protected byte[] extractorRecord(GenericRowRecord record) {
        return record.toString().getBytes(StandardCharsets.UTF_8);
    }

}
