package com.sf.bdp.extractor;

import com.sf.bdp.entity.GenericRowRecord;

import java.util.Map;

public class DynamicRecordExtractor extends BaseProducerRecordExtractor {


    public DynamicRecordExtractor(Map<String, String> tableTopicMap) {
        super(tableTopicMap);
    }


    @Override
    protected byte[] extractorRecord(GenericRowRecord record) {




        return new byte[0];
    }

}
