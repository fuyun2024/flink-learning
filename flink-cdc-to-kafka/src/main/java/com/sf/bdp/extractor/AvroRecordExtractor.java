package com.sf.bdp.extractor;


import com.sf.bdp.entity.GenericAvroRecord;
import com.sf.bdp.entity.GenericCdcRecord;
import com.sf.bdp.utils.RowTypeUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;


public class AvroRecordExtractor implements RecordExtractor<GenericCdcRecord, GenericAvroRecord> {


    @Override
    public GenericAvroRecord apply(GenericCdcRecord genericCdcRecord) {
        /**
         * 构建 schema
         */
        Schema debeziumSchema = buildSchema(genericCdcRecord.getFieldNames(), genericCdcRecord.getFieldTypes());

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
     * 获取数据 schema
     *
     * @param fieldNames
     * @param fieldTypes
     * @return
     */
    protected Schema buildSchema(String[] fieldNames, org.apache.kafka.connect.data.Schema.Type[] fieldTypes) {
        RowType rowType = RowTypeUtils.createRowType(fieldNames, fieldTypes);

        Schema schema = AvroSchemaConverter.convertToSchema(rowType);

        return getActualSchema(schema);
    }


    /**
     * 获取原始的 schema
     *
     * @param schema
     * @return
     */
    private Schema getActualSchema(Schema schema) {
        List<Schema> types = schema.getTypes();
        int size = types.size();
        if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
            return types.get(0);
        } else if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
            return types.get(1);
        } else {
            throw new IllegalArgumentException(
                    "The Avro schema is not a nullable type: " + schema.toString());
        }
    }


    /**
     * 构建 avro 数据
     *
     * @param genericCdcRecord
     * @param schema
     * @return
     */
    protected GenericRecord buildGenericRecord(GenericCdcRecord genericCdcRecord, Schema schema) {
        GenericRecord genericRecord = new GenericData.Record(schema);
        Object[] values = genericCdcRecord.getValues();
        String[] fieldNames = genericCdcRecord.getFieldNames();

        for (int i = 0; i < fieldNames.length; i++) {
            genericRecord.put(fieldNames[i], values[i]);
        }
        return genericRecord;
    }


}
