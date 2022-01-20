package com.sf.bdp.extractor;


import com.sf.bdp.record.GenericAvroRecord;
import com.sf.bdp.record.GenericCdcRecord;
import com.sf.bdp.utils.AvroSchemaConverter;
import com.sf.bdp.utils.RowTypeUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;

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
     * 获取 debezium schema
     *
     * @param fieldNames
     * @param fieldTypes
     * @return
     */
    private Schema buildSchema(String[] fieldNames, org.apache.kafka.connect.data.Schema.Type[] fieldTypes) {
        RowType rowType = RowTypeUtils.createRowType(fieldNames, fieldTypes);
        RowType debeziumAvroRowType = RowTypeUtils.createDebeziumAvroRowType(fromLogicalToDataType(rowType));
        return AvroSchemaConverter.convertToSchema(debeziumAvroRowType);
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
     * 构建数据
     *
     * @param genericCdcRecord
     * @param debeziumSchema
     * @return
     */
    private GenericRecord buildGenericRecord(GenericCdcRecord genericCdcRecord, Schema debeziumSchema) {
        Object[] values = genericCdcRecord.getValues();
        String[] fieldNames = genericCdcRecord.getFieldNames();

        Schema actualSchema = getActualSchema(debeziumSchema);


        GenericRecord genericRecord = new GenericData.Record(actualSchema);
        switch (genericCdcRecord.getKind()) {
            case INSERT:
            case UPDATE_AFTER:
                Schema afterSchema = actualSchema.getFields().get(1).schema();
                GenericRecord afterRecord = new GenericData.Record(getActualSchema(afterSchema));
                setRecord(afterRecord, fieldNames, values);
                genericRecord.put(0, null);
                genericRecord.put(1, afterRecord);
                genericRecord.put(2, new Utf8(OP_INSERT.toString()));
                return genericRecord;
            case UPDATE_BEFORE:
            case DELETE:
                Schema beforeSchema = actualSchema.getFields().get(0).schema();
                GenericRecord beforeRecord = new GenericData.Record(getActualSchema(beforeSchema));
                setRecord(beforeRecord, fieldNames, values);
                genericRecord.put(0, beforeRecord);
                genericRecord.put(1, null);
                genericRecord.put(2, new Utf8(OP_DELETE.toString()));
                return genericRecord;
            default:
                throw new UnsupportedOperationException(
                        format(
                                "Unsupported operation '%s' for row kind.",
                                genericCdcRecord.getKind()));
        }
    }

    private GenericRecord setRecord(GenericRecord genericRecord, String[] fieldNames, Object[] values) {
        for (int i = 0; i < fieldNames.length; i++) {
            genericRecord.put(fieldNames[i], values[i]);
        }
        return genericRecord;
    }


    /**
     * 包装 Debezium data Extract
     *
     * @param rowData
     * @return
     */
    private GenericRowData wrapDebeziumData(RowData rowData) {
        GenericRowData outputReuse = new GenericRowData(3);
        try {
            switch (rowData.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    outputReuse.setField(0, null);
                    outputReuse.setField(1, rowData);
                    outputReuse.setField(2, OP_INSERT);
                    return outputReuse;
                case UPDATE_BEFORE:
                case DELETE:
                    outputReuse.setField(0, rowData);
                    outputReuse.setField(1, null);
                    outputReuse.setField(2, OP_DELETE);
                    return outputReuse;
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
