package com.sf.bdp.extractor;


import com.sf.bdp.entity.GenericCdcRecord;
import com.sf.bdp.entity.GenericAvroRecord;
import com.sf.bdp.utils.RowTypeUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.RowType;

import static java.lang.String.format;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;


public class AvroRecordExtractor implements RecordExtractor<GenericCdcRecord, GenericAvroRecord> {

    @Override
    public GenericAvroRecord apply(GenericCdcRecord genericCdcRecord) {
        /**
         * 构建 schema
         */
        Schema debeziumSchema = buildDebeziumSchema(genericCdcRecord.getFieldNames(), genericCdcRecord.getFieldTypes());

        /**
         * 构建 record
         */
        GenericRecord genericRecord = buildGenericRecord(genericCdcRecord, debeziumSchema);

        GenericAvroRecord genericAvroRecord = new GenericAvroRecord();
        genericAvroRecord.setSchema(debeziumSchema);
        genericAvroRecord.setGenericRecord(genericRecord);
        return genericAvroRecord;
    }


    /**
     * 获取 debezium schema
     *
     * @param fieldNames
     * @param fieldTypes
     * @return
     */
    private Schema buildDebeziumSchema(String[] fieldNames, org.apache.kafka.connect.data.Schema.Type[] fieldTypes) {
        RowType rowType = RowTypeUtils.createRowType(fieldNames, fieldTypes);
        return AvroSchemaConverter.convertToSchema(rowType).getTypes().get(1);
    }


    private GenericRecord buildGenericRecord(GenericCdcRecord genericCdcRecord, Schema schema) {
        GenericRecord genericRecord = new GenericData.Record(schema);
        Object[] values = genericCdcRecord.getValues();
        String[] fieldNames = genericCdcRecord.getFieldNames();

        for (int i = 0; i < fieldNames.length; i++) {
            genericRecord.put(fieldNames[i], values[i]);
        }
        return genericRecord;
    }


}
