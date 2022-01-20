package com.sf.bdp.extractor.old;

import com.sf.bdp.record.GenericCdcRecord;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class StringRecordExtractor extends BaseRecordExtractor {


    public StringRecordExtractor(Map<String, String> tableTopicMap) {
        super(tableTopicMap);
    }

    @Override
    protected byte[] extractorRecord(GenericCdcRecord record) {
        return record.toString().getBytes(StandardCharsets.UTF_8);
    }

}
