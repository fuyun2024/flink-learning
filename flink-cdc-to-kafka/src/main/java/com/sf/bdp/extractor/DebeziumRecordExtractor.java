package com.sf.bdp.extractor;


import com.sf.bdp.entity.GenericCdcRecord;
import com.sf.bdp.entity.GenericAvroRecord;
import com.sf.bdp.utils.RowTypeUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import static java.lang.String.format;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;


public class DebeziumRecordExtractor implements RecordExtractor<GenericCdcRecord, GenericAvroRecord> {

    /**
     * insert operation.
     */
    private static final StringData OP_INSERT = StringData.fromString("c");

    /**
     * delete operation.
     */
    private static final StringData OP_DELETE = StringData.fromString("d");



    private transient GenericRowData outputReuse;


    @Override
    public GenericAvroRecord apply(GenericCdcRecord genericCdcRecord) {
        /**
         * 构建 schema
         */
        Schema debeziumSchema = buildDebeziumSchema(genericCdcRecord.getFieldNames(), genericCdcRecord.getFieldTypes());


        GenericRecord genericRecord = buildGenericRecord(genericCdcRecord, debeziumSchema);

//        final GenericRecord record = (GenericRecord) runtimeConverter.convert(schema, row);

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
        RowType debeziumAvroRowType = RowTypeUtils.createDebeziumAvroRowType(fromLogicalToDataType(rowType));
        return AvroSchemaConverter.convertToSchema(debeziumAvroRowType);
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


    public byte[] serialize(RowData rowData) {
        try {
            switch (rowData.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    outputReuse.setField(0, null);
                    outputReuse.setField(1, rowData);
                    outputReuse.setField(2, OP_INSERT);
//                    return avroSerializer.serialize(outputReuse);

                case UPDATE_BEFORE:
                case DELETE:
                    outputReuse.setField(0, rowData);
                    outputReuse.setField(1, null);
                    outputReuse.setField(2, OP_DELETE);
//                    return avroSerializer.serialize(outputReuse);
                default:
                    throw new UnsupportedOperationException(
                            format(
                                    "Unsupported operation '%s' for row kind.",
                                    rowData.getRowKind()));
            }
        } catch (Throwable t) {
            throw new RuntimeException(format("Could not serialize row '%s'.", rowData), t);
        }
    }


}
